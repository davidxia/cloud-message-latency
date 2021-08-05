package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"math"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	defaultBrokerHost     = "localhost"
	defaultBrokerPort     = 9092
	defaultTestDuration   = 10 * time.Minute
	defaultBurnInDuration = 1 * time.Minute
	clusterAPIKeyEnv      = "CLUSTER_API_KEY"
	clusterAPISecretEnv   = "CLUSTER_API_SECRET"
)

var once sync.Once

var (
	brokerHost     = flag.String("brokerHost", defaultBrokerHost, "Kafka broker host.")
	brokerPort     = flag.Int("brokerPort", defaultBrokerPort, "Kafka broker port.")
	topicID        = flag.String("topicID", "", "Topic ID. Required.")
	testDuration   = flag.Duration("testDuration", defaultTestDuration, "Test duration in minutes.")
	burnInDuration = flag.Duration("burnInDuration", defaultBurnInDuration, "Burn in duration in seconds.")
)

func init() {
	flag.Parse()
}

func setup() (consumer *kafka.Consumer, err error) {
	once.Do(func() {
		apiKey, exists := os.LookupEnv(clusterAPIKeyEnv)
		if !exists {
			log.Fatalf("you must set the %s environment variable", clusterAPIKeyEnv)
		}
		apiSecret, exists := os.LookupEnv(clusterAPISecretEnv)
		if !exists {
			log.Fatalf("you must set the %s environment variable", clusterAPISecretEnv)
		}

		conf := &kafka.ConfigMap{
			"bootstrap.servers": fmt.Sprintf("%s:%d", *brokerHost, *brokerPort),
			"group.id":          *topicID,
			"security.protocol": "SASL_SSL",
			"sasl.mechanisms":   "PLAIN",
			"sasl.username":     apiKey,
			"sasl.password":     apiSecret,
			"client.id":         *topicID,
		}

		consumer, err = kafka.NewConsumer(conf)
	})

	return
}

func main() {
	if *topicID == "" {
		log.Fatalf("-topicID must be set")
	}

	ctx := context.Background()
	consumer, err := setup()
	if err != nil {
		log.Fatalf("failed to create Kafka consumer: %s", err.Error())
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(ctx, *testDuration)
	defer cancel()

	summary := prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace: "kafka",
		Name:      "latency",
		Help:      "Confluent Apache Kafka latency in milliseconds",
		Objectives: map[float64]float64{
			0.5:      0.05,
			0.9:      0.01,
			0.99:     0.001,
			0.999:    0.0001,
			0.9999:   0.00001,
			0.99999:  0.000001,
			0.999999: 0.0000001,
		},
	})
	prometheus.MustRegister(summary)

	if err = consumer.Subscribe(*topicID, nil); err != nil {
		log.Fatalf("failed to subscribe to topic %s: %s", *topicID, err.Error())
	}

	burnInStart := time.Now().Unix()

	go consume(consumer, burnInStart, summary)

	// Expose the registered metrics via HTTP.
	http.Handle("/metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		},
	))
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func consume(consumer *kafka.Consumer, burnInStart int64, summary prometheus.Summary) {
	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}

		receiptTime := time.Now().UnixNano()
		publishTime := msg.Timestamp.UnixNano()

		latencyMillis := float64(receiptTime-publishTime) / math.Pow(10, 6)
		burnInDurationLeft := int64(burnInDuration.Seconds()) - (time.Now().Unix() - burnInStart)
		if burnInDurationLeft < 0 {
			summary.Observe(latencyMillis)
		} else {
			log.Printf("burning in for %d more seconds", burnInDurationLeft)
		}
		// Uncomment next line for debugging
		//log.Printf("got message at %d, latency %.0f ms", receiptTime, latencyMillis)
	}
}
