FROM python:3.9.7-slim
RUN pip3 install pika

COPY trips_counter/trips_counter.py /opt/app/trips_counter.py
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]