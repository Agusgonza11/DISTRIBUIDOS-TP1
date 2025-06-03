SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	GOOS=linux go build -o bin/client github.com/7574-sistemas-distribuidos/docker-compose-init/client
.PHONY: build

docker-image:
	docker build -f ./dockers/input_gateway.Dockerfile -t "input_gateway:latest" .
	docker build -f ./dockers/output_gateway.Dockerfile -t "output_gateway:latest" .
	docker build -f ./dockers/filter.Dockerfile -t filter:latest .
	docker build -f ./dockers/joiner.Dockerfile -t joiner:latest .
	docker build -f ./dockers/aggregator.Dockerfile -t aggregator:latest .
	docker build -f ./dockers/pnl.Dockerfile -t pnl:latest .
	docker build -f ./dockers/client.Dockerfile -t "client:latest" .
	docker build -f ./dockers/broker.Dockerfile -t "broker:latest" .


	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	@for svc in $$(docker compose -f docker-compose-dev.yaml ps --services); do \
		container_id=$$(docker compose -f docker-compose-dev.yaml ps -q $$svc); \
		docker exec $$container_id touch /tmp/shutdown_global.flag || true; \
	done
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
