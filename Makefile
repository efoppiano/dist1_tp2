SHELL := /bin/bash

default: up logs
.PHONY: default

up: docker-compose-up
.PHONY: up
stop: docker-compose-stop
.PHONY: stop
down: docker-compose-down
.PHONY: down
logs: docker-compose-logs
.PHONY: logs
ps: docker-compose-ps
.PHONY: ps
rc: restart-client
.PHONY: rc

docker-compose-up: docker-compose-down
	python3 scripts/build.py
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-stop:
	docker compose -f docker-compose-dev.yaml stop -t 1
.PHONY: docker-compose-stop

docker-compose-down: docker-compose-stop
	docker compose -f docker-compose-dev.yaml down --volumes --remove-orphans
	sudo rm -rf .volumes
.PHONY: docker-compose-down


docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

client-logs:
	docker compose -f docker-compose-dev.yaml logs -f | grep tp2-client
.PHONY: client-logs

docker-compose-ps:
	docker compose -f docker-compose-dev.yaml ps
.PHONY: docker-compose-ps


client ?= original
restart-client:
	docker compose -f docker-compose-dev.yaml restart tp2-client_$(client)-1
.PHONY: restart-client


tests:
	docker compose -f docker-compose-tests.yaml up --build
.PHONY: tests