FROM python:3-slim

WORKDIR /usr/src/app

COPY setup.py /usr/src/app/
RUN pip install .

COPY prometheus_kafka_consumer_group_exporter/*.py /usr/src/app/prometheus_kafka_consumer_group_exporter/
RUN pip install -e .

COPY LICENSE /usr/src/app/
COPY README.md /usr/src/app/

EXPOSE 9208

ENTRYPOINT ["python", "-u", "/usr/local/bin/prometheus-kafka-consumer-group-exporter"]
