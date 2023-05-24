FROM python:3.9.7-slim
RUN pip3 install pika

COPY dist_mean_calculator/dist_mean_calculator.py /opt/app/dist_mean_calculator.py
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]