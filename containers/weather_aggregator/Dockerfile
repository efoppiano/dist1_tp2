FROM python:3.9.7-slim
RUN pip3 install pika
RUN pip3 install haversine

COPY weather_aggregator/weather_aggregator.py /opt/app/weather_aggregator.py
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]