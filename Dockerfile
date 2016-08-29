FROM python:3-slim

WORKDIR /usr/src/app

COPY prometheus_kafka_consumer_group_exporter/*.py /usr/src/app/prometheus_kafka_consumer_group_exporter/
COPY setup.py /usr/src/app/
COPY LICENSE /usr/src/app/
COPY README.md /usr/src/app/
COPY MANIFEST.in /usr/src/app/

RUN pip install -e .

EXPOSE 8080

ENTRYPOINT ["python", "-u", "/usr/local/bin/prometheus-kafka-consumer-group-exporter"]
