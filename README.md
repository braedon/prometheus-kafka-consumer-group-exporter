Prometheus Kafka Consumer Group Exporter
====
This Prometheus exporter consumes the `__consumer_offsets` topic of a Kafka cluser and exports the results as Prometheus gauge metrics. i.e. it shows the position of Kafka consumer groups.

The high-water marks of the partitions of each topic are also exported.

# Installation
The exporter requires Python 3 and Pip 3 to be installed.

To install the latest published version via Pip, run:
```
> pip3 install prometheus-kafka-consumer-group-exporter
```
Note that you may need to add the start script location (see pip output) to your `PATH`.

# Usage
Once installed, you can run the exporter with the `prometheus-kafka-consumer-group-exporter` command.

By default, it will bind to port 9208 and connect to Kafka on `localhost:9092`. You can change these defaults as required by passing in arguments:
```
> prometheus-kafka-consumer-group-exporter -p <port> -b <kafka nodes>
```
Run with the `-h` flag to see details on all the available arguments.

Prometheus metrics can then be scraped from the `/metrics` path, e.g. http://localhost:9208/metrics. Metrics are currently actually exposed on all paths, but this may change in the future and `/metrics` is the standard path for Prometheus metric endpoints.

# Metrics
Four main metrics are exported:

### `kafka_consumer_group_offset{group, topic, partition}`
The latest committed offset of a consumer group in a given partition of a topic, as read from `__consumer_offsets`. Useful for calculating the consumption rate and lag of a consumer group.

### `kafka_consumer_group_commits{group, topic, partition}`
The number of commit messages read from `__consumer_offsets` by the exporter from a consumer group for a given partition of a topic. Useful for calculating the commit rate of a consumer group (i.e. are the consumers working).

### `kafka_consumer_group_exporter_offset{partition}`
The offset of the exporter's consumer in each partition of the `__consumer_offset` topic. Useful for calculating the lag of the exporter.

### `kafka_topic_highwater{topic, partition}`
The offset of the head of a given partition of a topic, as reported by the lead broker for the partition. Useful for calculating the production rate of the producers for a topic, and the lag of a consumer group (or the exporter itself).

## Lag
While a lag metric isn't exported, it can be calculated using other metrics:
```
# Lag for a consumer group:
kafka_topic_highwater - on (topic, partition) kafka_consumer_group_offset{group="some-consumer-group"}

# Lag for the exporter:
kafka_topic_highwater{topic='__consumer_offsets'} - on (partition) kafka_consumer_group_exporter_offset
```
Note that as the offset and high-water metrics are updated separately the offset value can be more up-to-date than the high-water, resulting in a negative lag. This is often the case with the exporter lag, as the exporter offset is tracked internally rather than read from `__consumer_offsets`.

# Kafka Config
If you need to set Kafka consumer configuration that isn't supported by command line arguments, you can provided a standard Kafka consumer properties file:
```
> prometheus-kafka-consumer-group-exporter --consumer-config consumer.properties
```
See the [Kafka docs](https://kafka.apache.org/documentation/#newconsumerconfigs) for details on consumer properties. However, as the exporter doesn't use the official consumer implementation, all properties may not be supported. Check the [kafka-python docs](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafkaconsumer) if you run into problems.

You can provide multiple files if that's helpful - they will be merged together, with later files taking precedence:
```
> prometheus-kafka-consumer-group-exporter --consumer-config consumer.properties --consumer-config another-consumer.properties
```
Note that where a command line argument relates to a consumer property (e.g. `--bootstrap-brokers` sets `bootstrap.servers`) a value provided via that argument will override any value for that property in a properties file. The argument default will only be used if the property isn't provided in either a file or an argument.

# Docker
Docker images for released versions can be found on Docker Hub (note that no `latest` version is provided):
```
> sudo docker pull braedon/prometheus-kafka-consumer-group-exporter:<version>
```
To run a container successfully, you will need map container port 9208 to a port on the host. Any options placed after the image name (`prometheus-kafka-consumer-group-exporter`) will be passed to the process inside the container. For example, you will need to use this to configure the kafka node(s) using `-b`.
```
> sudo docker run --rm --name exporter \
    -p <host port>:9208 \
    braedon/prometheus-kafka-consumer-group-exporter:<version> -b <kafka nodes>
```

# Development
To install directly from the git repo, run the following in the root project directory:
```
> pip3 install .
```
The exporter can be installed in "editable" mode, using pip's `-e` flag. This allows you to test out changes without having to re-install.
```
> pip3 install -e .
```

To build a docker image directly from the git repo, run the following in the root project directory:
```
> sudo docker build -t <your repository name and tag> .
```
Send me a PR if you have a change you want to contribute!
