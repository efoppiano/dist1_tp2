FROM python:3.9.7-slim
RUN pip3 install pika

COPY station_aggregator/station_aggregator.py /opt/app/station_aggregator.py
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]