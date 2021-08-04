package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"context"
	"flag"
	"fmt"
	"google.golang.org/api/iterator"
	"log"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	defaultProjectID      = "my-project"
	defaultTopicPrefix    = "test"
	defaultRPS            = 100
	defaultDuration       = 5 * time.Minute
	defaultResourceExpiry = 1 * time.Hour
	defaultTestLabelKey   = "test"
	defaultMessageSize    = 1e2
)

// once guards cleanup related operations in setup. No need to set up and tear
// down every time, so this speeds things up.
var (
	once           sync.Once
	topicID        string
	subscriptionID string
)

var (
	projectID      = flag.String("projectID", defaultProjectID, "Project ID.")
	topicPrefix    = flag.String("topicPrefix", defaultTopicPrefix, "Topic ID.")
	duration       = flag.Duration("duration", defaultDuration, "Test duration.")
	messageSize    = flag.Int("messageSize", defaultMessageSize, "Message size in bytes.")
	rps            = flag.Int("RPS", defaultRPS, "Messages to publish per second.")
	resourceExpiry = flag.Duration("resourceExpiry", defaultResourceExpiry, "Resource expiry in hours. Topics and subscriptions older than this will be deleted.")
	testLabelKey   = flag.String("testLabelKey", defaultTestLabelKey, "Topic label key that indicates it was created from this test.")
)

func main() {
	flag.Parse()

	ctx := context.Background()
	client := setup()
	defer client.Close()

	topic, err := client.CreateTopicWithConfig(ctx, topicID, &pubsub.TopicConfig{
		Labels: map[string]string{
			*testLabelKey: fmt.Sprintf("%d", time.Now().UnixNano()),
		},
	})
	if err != nil {
		log.Fatalf("failed to create topicID %v in projectID %s: %v", topicID, *projectID, err)
	}
	log.Printf("created topicID: %v", topic)

	subConfig := pubsub.SubscriptionConfig{
		Labels: map[string]string{
			*testLabelKey: fmt.Sprintf("%d", time.Now().UnixNano()),
		},
		Topic: topic,
		RetainAckedMessages:   false,
		EnableMessageOrdering: false,
		DeadLetterPolicy:      nil,
		Detached:              false,
	}
	sub, err := client.CreateSubscription(ctx, subscriptionID, subConfig)
	if err != nil {
		log.Fatalf("failed to create subscriptionID: %v", subscriptionID)
	}
	log.Printf("created subscriptionID: %v", sub)

	fmt.Printf("press enter to start publishing %d byte messages at %d RPS for %v...\n", *messageSize, *rps, duration)
	_, err = fmt.Scanln()
	if err != nil {
		log.Fatal(err)
	}

	ticker := time.NewTicker(time.Duration(1000 / *rps) * time.Millisecond)
	done := make(chan bool)

	var publishedMsgs, numErrors uint64
	publishedMsgsChan := make(chan bool, 10)
	errorsChan := make(chan bool, 10)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-publishedMsgsChan:
				atomic.AddUint64(&publishedMsgs, 1)
			case <-errorsChan:
				atomic.AddUint64(&numErrors, 1)
			}
		}
	}()

	go dispatchPublisher(ctx, topic, done, ticker, *messageSize, publishedMsgsChan, errorsChan)

	time.Sleep(*duration)
	ticker.Stop()
	done <- true

	log.Printf("successfully published %d messages, failed %d ", publishedMsgs, numErrors)
}

func setup() *pubsub.Client {
	ctx := context.Background()

	var err error
	client, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	once.Do(func() {
		topicID = fmt.Sprintf("%s-%d", *topicPrefix, time.Now().UnixNano())
		subscriptionID = topicID

		// Delete topics and subscriptions from previous tests.
		it := client.Topics(ctx)
		for {
			t, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return
			}
			tConfig, err := t.Config(ctx)
			if err != nil {
				log.Printf("unable to get config for topic %s. Skipping", t)
				continue
			}

			// Only delete topics created from these tests that have a specific label key
			testLabelVal, ok := tConfig.Labels[*testLabelKey]
			if !ok {
				log.Printf("topic %s test label %s not present. Skipping", t, *testLabelKey)
				continue
			}

			timestamp, err := strconv.ParseInt(testLabelVal, 10, 64)
			if err != nil {
				continue
			}
			timeTCreated := time.Unix(0, timestamp)
			if time.Since(timeTCreated) > *resourceExpiry {
				if err := t.Delete(ctx); err != nil {
					log.Printf("delete topic err: %v: %v", t, err)
				} else {
					log.Printf("deleted topic %s", t)
				}
			}
		}

		is := client.Subscriptions(ctx)
		for {
			s, err := is.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return
			}
			sConfig, err := s.Config(ctx)
			if err != nil {
				log.Printf("unable to get config for subscription %s. Skipping", s)
				continue
			}

			// Only delete subscriptions created from these tests that have a specific label key
			testLabelVal, ok := sConfig.Labels[*testLabelKey]
			if !ok {
				log.Printf("subscription %s test label %s not present. Skipping", s, *testLabelKey)
				continue
			}

			// Only delete subscriptions created from these tests.
			timestamp, err := strconv.ParseInt(testLabelVal, 10, 64)
			if err != nil {
				continue
			}
			timeTCreated := time.Unix(0, timestamp)
			if time.Since(timeTCreated) > *resourceExpiry {
				if err := s.Delete(ctx); err != nil {
					log.Printf("delete subscription err: %v: %v", s, err)
				} else {
					log.Printf("deleted subscription %s", s)
				}
			}
		}
	})

	return client
}

func dispatchPublisher(ctx context.Context, topic *pubsub.Topic, done chan bool, ticker *time.Ticker, msgSize int, publishedMsgsChan, numErrsChan chan bool) {
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			go publish(ctx, topic, msgSize, publishedMsgsChan, numErrsChan)
		}
	}
}

func publish(ctx context.Context, topic *pubsub.Topic, msgSize int, publishedMsgsChan, numErrsChan chan bool) {
	attrs := map[string]string{
		// The pt attribute is the publication time in nanoseconds since epoch.
		"pt": strconv.FormatInt(time.Now().UnixNano(), 10),
	}
	data := bytes.Repeat([]byte("A"), int(math.Max(float64(msgSize-int(unsafe.Sizeof(attrs))-25), 1)))
	msg := pubsub.Message{
		Data:       data,
		Attributes: attrs,
	}

	result := topic.Publish(ctx, &msg)

	// The Get method blocks until a server-generated ID or
	// an error is returned for the published message.
	_, err := result.Get(ctx)
	if err != nil {
		numErrsChan <- true
		return
	}

	publishedMsgsChan <- true
}
