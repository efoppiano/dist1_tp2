FROM python:3.9.7-slim
RUN pip3 install pika

COPY prec_filter/prec_filter.py /opt/app/prec_filter.py
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]