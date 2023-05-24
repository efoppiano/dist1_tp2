FROM python:3.9.7-slim
RUN pip3 install pika
RUN pip3 install pyzmq

COPY client/client.py /opt/app/client.py
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]