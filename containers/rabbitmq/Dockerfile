FROM rabbitmq:3.9.16-management-alpine

RUN mkdir -p /etc/rabbitmq/conf.d
RUN chown -R rabbitmq:rabbitmq /etc/rabbitmq/conf.d
COPY rabbit-logging.conf /etc/rabbitmq/conf.d

EXPOSE 15672