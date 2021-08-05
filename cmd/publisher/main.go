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

func init() {
	flag.Parse()
}

func main() {
	ctx := context.Background()
	client := setup()
	defer client.Close()

	topic, err := client.CreateTopicWithConfig(ctx, topicID, &pubsub.TopicConfig{
		Labels: map[string]string{
			*testLabelKey: fmt.Sprintf("%d", time.Now().UnixNano()),
		},
	})
	if err != nil {
		log.Fatalf("failed to create topicID %v in projectID %s: %s", topicID, *projectID, err)
	}
	log.Printf("created topicID: %+v", topic)

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
	if _, err = fmt.Scanln(); err != nil {
		log.Fatal(err)
	}

	ticker := time.NewTicker(time.Duration(1000 / *rps) * time.Millisecond)

	var publishedMsgs, numErrors uint64
	publishedMsgsChan := make(chan bool, 10)
	errorsChan := make(chan bool, 10)
	counterDone := make(chan bool)

	go counter(counterDone, publishedMsgsChan, errorsChan, publishedMsgs, numErrors)

	dispatcherDone := make(chan bool)
	go dispatchPublisher(ctx, topic, dispatcherDone, ticker, *messageSize, publishedMsgsChan, errorsChan)

	time.Sleep(*duration)
	ticker.Stop()
	counterDone <- true
	dispatcherDone <- true

	log.Printf("successfully published %d messages, failed %d ", publishedMsgs, numErrors)
}

func expired(timestamp int64, expiry time.Duration) bool {
	created := time.Unix(0, timestamp)
	return time.Since(created) > expiry
}


func deleteOldResources(ctx context.Context, client *pubsub.Client) {
	topics := client.Topics(ctx)
	for {
		topic, err := topics.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("unable to get topics: %s", err)
			break
		}

		tConfig, err := topic.Config(ctx)
		if err != nil {
			log.Printf("unable to get config for topic %s. Skipping", topic)
			continue
		}

		// Only delete topics created from these tests that have a specific label key
		testLabelVal, ok := tConfig.Labels[*testLabelKey]
		if !ok {
			log.Printf("topic %s test label %s not present. Skipping", topic, *testLabelKey)
			continue
		}

		timestamp, err := strconv.ParseInt(testLabelVal, 10, 64)
		if err != nil {
			continue
		}

		if expired(timestamp, *resourceExpiry) {
			if err := topic.Delete(ctx); err != nil {
				log.Printf("error deleting topic: %v: %s", topic, err)
			} else {
				log.Printf("deleted topic %s", topic)
			}
		}
	}

	subscriptions := client.Subscriptions(ctx)
	for {
		subscription, err := subscriptions.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("unable to get subscriptions: %s", err)
			break
		}

		sConfig, err := subscription.Config(ctx)
		if err != nil {
			log.Printf("unable to get config for subscription %s. Skipping", subscription)
			continue
		}

		// Only delete subscriptions created from these tests that have a specific label key
		testLabelVal, ok := sConfig.Labels[*testLabelKey]
		if !ok {
			log.Printf("subscription %s test label %s not present. Skipping", subscription, *testLabelKey)
			continue
		}

		// Only delete subscriptions created from these tests.
		timestamp, err := strconv.ParseInt(testLabelVal, 10, 64)
		if err != nil {
			continue
		}

		if expired(timestamp, *resourceExpiry) {
			if err := subscription.Delete(ctx); err != nil {
				log.Printf("error deleting subscription: %v: %s", subscription, err)
			} else {
				log.Printf("deleted subscription %s", subscription)
			}
		}
	}
}

func setup() *pubsub.Client {
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("failed to create client: %s", err)
	}

	once.Do(func() {
		topicID = fmt.Sprintf("%s-%d", *topicPrefix, time.Now().UnixNano())
		subscriptionID = topicID

		deleteOldResources(ctx, client)
	})

	return client
}

func counter(done, publishedMsgsChan, errorsChan chan bool, publishedMsgs, numErrors uint64) {
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
}

func dispatchPublisher(ctx context.Context, topic *pubsub.Topic, done chan bool, ticker *time.Ticker, msgSize int, publishedMsgsChan, numErrsChan chan bool) {
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			// If a tick comes in, publish a message.
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
	if _, err := result.Get(ctx); err != nil {
		numErrsChan <- true
		return
	}

	publishedMsgsChan <- true
}
