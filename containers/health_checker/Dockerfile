FROM python:3.9.7-slim
RUN pip3 install pika

COPY health_checker/health_checker.py /opt/app/health_checker.py
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]