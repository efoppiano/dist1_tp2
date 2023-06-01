FROM python:3.9.7-slim
RUN pip3 install pika

COPY prec_filter/ /opt/app/prec_filter
COPY year_filter/ /opt/app/year_filter
COPY _tests/main.py /opt/app/main.py
COPY _tests/tests_dir /opt/app/tests_dir
COPY common /opt/app/common

ENTRYPOINT ["/bin/bash"]