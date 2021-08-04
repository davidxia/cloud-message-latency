package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultBrokerHost     = "localhost"
	defaultBrokerPort     = 9092
	defaultTopicPrefix    = "test"
	defaultRPS            = 100
	defaultDuration       = 5 * time.Minute
	defaultResourceExpiry = 1 * time.Hour
	defaultMessageSize    = 1e2
	clusterAPIKeyEnv      = "CLUSTER_API_KEY"
	clusterAPISecretEnv   = "CLUSTER_API_SECRET"
)

// once guards cleanup related operations in setup. No need to set up and tear
// down every time, so this speeds things up.
var (
	once    sync.Once
	topicID string
)

var (
	brokerHost     = flag.String("brokerHost", defaultBrokerHost, "Kafka broker host.")
	brokerPort     = flag.Int("brokerPort", defaultBrokerPort, "Kafka broker port.")
	topicPrefix    = flag.String("topicPrefix", defaultTopicPrefix, "Topic prefix.")
	duration       = flag.Duration("duration", defaultDuration, "Test duration.")
	messageSize    = flag.Int("messageSize", defaultMessageSize, "Message size in bytes.")
	rps            = flag.Int("RPS", defaultRPS, "Messages to produce per second.")
	resourceExpiry = flag.Duration("resourceExpiry", defaultResourceExpiry, "Resource expiry in hours. Topics and subscriptions older than this will be deleted.")
)

func init() {
	flag.Parse()
}

func setup() (producer *kafka.Producer, err error) {
	once.Do(func() {
		apiKey, exists := os.LookupEnv(clusterAPIKeyEnv)
		if !exists {
			log.Fatalf("you must set the %s environment variable", clusterAPIKeyEnv)
		}
		apiSecret, exists := os.LookupEnv(clusterAPISecretEnv)
		if !exists {
			log.Fatalf("you must set the %s environment variable", clusterAPISecretEnv)
		}

		ctx := context.Background()
		conf := &kafka.ConfigMap{
			"bootstrap.servers": fmt.Sprintf("%s:%d", *brokerHost, *brokerPort),
			"security.protocol": "SASL_SSL",
			"sasl.mechanisms":   "PLAIN",
			"sasl.username":     apiKey,
			"sasl.password":     apiSecret,
		}
		adminClient, err := kafka.NewAdminClient(conf)
		if err != nil {
			log.Fatalf("failed to create Kafka admin client: %s", err)
		}

		deleteOldResources(ctx, adminClient)

		topicID = fmt.Sprintf("%s-%d", *topicPrefix, time.Now().UnixNano())
		topicsSpec := []kafka.TopicSpecification{
			{
				Topic:             topicID,
				NumPartitions:     8,
				ReplicationFactor: 0,
				//Config:            nil,
			},
		}
		if _, err := adminClient.CreateTopics(ctx, topicsSpec); err != nil {
			log.Fatalf("failed to create topics %v: %s", topicsSpec, err)
		}
		log.Printf("created topic %s", topicID)
		producer, err = kafka.NewProducer(conf)
	})

	return
}

func main() {
	producer, err := setup()
	if err != nil {
		log.Fatalf("Failed to create the Kafka cluster manager: %s", err.Error())
	}
	defer producer.Close()

	fmt.Printf("press enter to start producing %d byte messages at %d RPS for %v...\n", *messageSize, *rps, duration)
	_, err = fmt.Scanln()
	if err != nil {
		log.Fatal(err)
	}

	ticker := time.NewTicker(time.Duration(1000 / *rps) * time.Millisecond)

	var producedMsgs, numErrors uint64
	producedMsgsChan := make(chan bool, 10)
	errorsChan := make(chan bool, 10)
	counterDone := make(chan bool)

	go counter(counterDone, producedMsgsChan, errorsChan, producedMsgs, numErrors)

	dispatcherDone := make(chan bool)
	go dispatchProducer(producer, dispatcherDone, ticker, producedMsgsChan, errorsChan)

	time.Sleep(*duration)
	ticker.Stop()
	counterDone <- true
	dispatcherDone <- true

	log.Printf("successfully produced %d messages, failed %d ", producedMsgs, numErrors)
}

func counter(done, producedMsgsChan, errorsChan chan bool, producedMsgs, numErrors uint64) {
	for {
		select {
		case <-done:
			return
		case <-producedMsgsChan:
			atomic.AddUint64(&producedMsgs, 1)
		case <-errorsChan:
			atomic.AddUint64(&numErrors, 1)
		}
	}
}

func expired(timestamp int64, expiry time.Duration) bool {
	created := time.Unix(0, timestamp)
	return time.Since(created) > expiry
}

func deleteOldResources(ctx context.Context, adminClient *kafka.AdminClient) {
	metadata, err := adminClient.GetMetadata(nil, true, 1e4)
	if err != nil {
		log.Fatalf("Failed to list topics: %s", err)
	}

	var expiredTopics []string
	for topic := range metadata.Topics {
		parts := strings.Split(topic, "-")
		if len(parts) != 2 || parts[0] != *topicPrefix {
			continue
		}

		timestamp, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}

		if expired(timestamp, *resourceExpiry) {
			expiredTopics = append(expiredTopics, topic)
		}
	}

	if len(expiredTopics) == 0 {
		return
	}

	_, err = adminClient.DeleteTopics(ctx, expiredTopics)
	if err != nil {
		log.Printf("error deleting topics: %v: %s", expiredTopics, err)
	} else {
		log.Printf("deleted expired topics %v", expiredTopics)
	}
}

func dispatchProducer(producer *kafka.Producer, done chan bool, ticker *time.Ticker, producedMsgsChan, numErrsChan chan bool) {
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			go produce(producer, producedMsgsChan, numErrsChan)
		}
	}
}

func produce(producer *kafka.Producer, producedMsgsChan, numErrsChan chan bool) {
	// Produce messages to topic (asynchronously)
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		if err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicID, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil); err != nil {
			log.Printf("error producing message: %s", err)
			numErrsChan <- true
		} else {
			log.Printf("published %s", word)
			producedMsgsChan <- true
		}
	}
	//attrs := map[string]string{
	//	// The pt attribute is the production time in nanoseconds since epoch.
	//	"pt": strconv.FormatInt(time.Now().UnixNano(), 10),
	//}
	//data := bytes.Repeat([]byte("A"), int(math.Max(float64(msgSize-int(unsafe.Sizeof(attrs))-25), 1)))
	//msg := pubsub.Message{
	//	Data:       data,
	//	Attributes: attrs,
	//}

}
