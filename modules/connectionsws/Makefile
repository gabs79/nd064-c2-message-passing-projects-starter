IMAGE_NAME := gabs79/udacity/connectionsws

.PHONY: usage
usage:
	echo 'Usage: [build] [push] [run run-arg=<>]'

.PHONY: build
build:
	docker build -t ${IMAGE_NAME} .

.PHONY: push
push:
	docker push ${IMAGE_NAME}

.PHONY: run
run:
	docker run --rm -d ${IMAGE_NAME} ${run-arg}
