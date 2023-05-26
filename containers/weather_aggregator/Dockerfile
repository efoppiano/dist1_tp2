FROM python:3.9.7-slim
RUN pip3 install pika
RUN pip3 install haversine

COPY weather_aggregator/ /opt/app/
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]