FROM python:3.9.7-slim
RUN pip3 install pika

COPY dist_mean_provider/dist_mean_provider.py /opt/app/dist_mean_provider.py
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]