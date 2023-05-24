FROM python:3.9.7-slim
RUN pip3 install pika
RUN pip3 install pyzmq

COPY gateway/gateway.py /opt/app/gateway.py
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]