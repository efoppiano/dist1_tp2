FROM python:3.9.7-slim
RUN pip3 install pika
RUN pip3 install pyzmq

COPY response_provider/response_provider.py /opt/app/response_provider.py
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]