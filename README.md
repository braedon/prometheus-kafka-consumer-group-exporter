Prometheus Kafka Consumer Group Exporter
====
This Prometheus exporter consumes the `__consumer_offsets` topic of a Kafka cluser and exports the results as Prometheus gauge metrics. i.e. it shows the position of Kafka consumer groups.

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
