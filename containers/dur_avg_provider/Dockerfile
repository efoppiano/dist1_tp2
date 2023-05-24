FROM python:3.9.7-slim
RUN pip3 install pika

COPY dur_avg_provider/dur_avg_provider.py /opt/app/dur_avg_provider.py
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]