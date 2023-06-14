FROM python:3.9.7-alpine
RUN pip3 install pika
RUN apk update
RUN apk add docker

COPY health_checker/*.py /opt/app/
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]