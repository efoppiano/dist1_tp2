FROM python:3.9.7-slim
RUN pip3 install pika
RUN pip3 install pyyaml

COPY synchronizer/synchronizer.py /opt/app/synchronizer.py
COPY synchronizer/config.yaml /opt/app/config.yaml
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]