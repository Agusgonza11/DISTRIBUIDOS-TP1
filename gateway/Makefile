SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/tp1-sistemas-distribuidos

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	GOOS=linux go build -o bin/gateway github.com/tp1-sistemas-distribuidos
.PHONY: build

docker-image:
	docker build -f ./Dockerfile -t "gateway:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during gateway build (your hard drive will like this :) ). Don't left uncommented if you
	# want to avoid rebuilding gateway image every time the docker-compose-up command
	# is executed, even when gateway code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose -f docker-compose.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose.yaml stop -t 1
	docker-compose -f docker-compose.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

up: docker-compose-up
.PHONY: up

down: docker-compose-down
.PHONY: down