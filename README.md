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

By default, it will bind to port 8080 and connect to Kafka on `localhost:9092`. You can change these defaults as required by passing in options:
```
> prometheus-kafka-consumer-group-exporter -p <port> -b <kafka nodes>
```
Run with the `-h` flag to see details on all the available options.

# Docker
Docker images for released versions can be found on Docker Hub (note that no `latest` version is provided):
```
> sudo docker pull braedon/prometheus-kafka-consumer-group-exporter:<version>
```
To run a container successfully, you will need map container port 8080 to a port on the host. Any options placed after the image name (`prometheus-kafka-consumer-group-exporter`) will be passed to the process inside the container. For example, you will need to use this to configure the kafka node(s) using `-b`.
```
> sudo docker run --rm --name exporter \
    -p <host port>:8080 \
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
