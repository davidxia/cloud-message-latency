# message-latency

Code here benchmarks message delivery latency with various services listed below.
This tool uses Go and the official upstream client libraries.

* [Apache Kafka][kafka]
* [Google Cloud Pub/Sub][pubsub]

## Dependencies

### gcloud and Google Cloud Pub/Sub

Install [`gcloud`][gcloud-install]. Login with a Google Cloud Platform account that has IAM permissions to list, create,
and delete Pub/Sub topics and subscriptions.

```
gcloud auth login --account [your@email.com]
gcloud auth application-default login --account [your@email.com]
```

### Golang

Install some decently recent version of [Golang][go-install]. You might also need to install a package that provides
the `gcc` compiler. The build-essential package provides this for Ubuntu/Debian.


## Recommended Usage

* Use separate machines for load generation and consumption.
* Run `scripts/./install.sh` to compile Go binaries. Default target OS is `darwin`. You can change to others like `linux`.

### Google Cloud Pub/Sub

* Google designed Cloud Pub/Sub for [high message volumes][pubsub-architecture]. Low volumes will cause higher latency.
This has to do with how Pub/Sub balances the pull requests between listeners.

### Publish

#### On Pub/Sub

```
bin/pubsub/publisher \
  -projectID warpspeed-testing \
  -topicPrefix dxiatest \
  -testLabelKey dxiatest

2021/07/29 23:28:40 created topicID: projects/warpspeed-testing/topics/dxiatest-1627601318562471531
2021/07/29 23:28:41 created subscriptionID: projects/warpspeed-testing/subscriptions/dxiatest-1627601318562471531
press enter to start publishing messages at 100 RPS...
[Before continuning, start the subscribers with the subscription ID above after the last slash. See below.]

2021/07/29 23:34:07 successfully published 8107 messages, failed 0
```

See more options with `-h`.

#### On Kafka

```
bin/kafka/./producer \
  -brokerHost my-broker.com \
  -brokerPort 9092 \
  -topicPrefix dxiatest \
  -messageSize 100 \
  -RPS 1000

2021/08/05 20:36:44 created topic dxiatest-1628195803751335869
press enter to start producing 100 byte messages at 1000 RPS for 5m0s...

```

### Pull-based Subscribe

```
bin/subscriber \
  -projectID warpspeed-testing \
  -subscriptionID dxiatest-1627601318562471531

...
2021/07/29 23:39:02 got message 1627601942927737706 at 1627601942955833627, latency 28 ms
2021/07/29 23:39:03 burning in for 35 more seconds
2021/07/29 23:39:03 got message 1627601943118298744 at 1627601943143695090, latency 25 ms
2021/07/29 23:39:03 burning in for 35 more seconds
...
2021/07/29 23:41:12 got message 1627602072178202997 at 1627602072206435115, latency 28 ms
2021/07/29 23:41:12 got message 1627602072447232089 at 1627602072490150539, latency 43 ms
2021/07/29 23:41:12 got message 1627602072487471149 at 1627602072523003862, latency 36 ms
2021/07/29 23:41:12 got message 1627602072566998024 at 1627602072678355582, latency 111 ms
2021/07/29 23:41:12 got message 1627602072724468599 at 1627602072750649353, latency 26 ms
2021/07/29 23:41:12 got message 1627602072815071921 at 1627602072854578547, latency 40 ms
...
```

See more options with `-h`.

#### Metrics

You can get latency quantiles while the subscriber is running.

```
watch -n 5 'curl --silent localhost:8080/metrics | grep pubsub'

Every 5.0s: curl localhost:8080/metrics | grep pubsub

pubsub_latency{quantile="0.5"} 26.640585
pubsub_latency{quantile="0.9"} 46.789227
pubsub_latency{quantile="0.99"} 470.668934
pubsub_latency_sum 68824.19071099993
pubsub_latency_count 1460
```

[kafka]: https://kafka.apache.org/
[pubsub]: https://cloud.google.com/pubsub/
[pubsub-architecture]: https://cloud.google.com/pubsub/architecture#basic_architecture
[gcloud-install]: https://cloud.google.com/sdk/docs/install
[go-install]: https://golang.org/doc/install
