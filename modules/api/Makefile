IMAGE_NAME := gabs79/udacity-gatewayws
TAG := v1.0.1

.PHONY: usage
usage:
	echo 'Usage: [build] [push] [run run-arg=<>]'

.PHONY: build
build:
	docker build -t ${IMAGE_NAME}:${TAG} .

.PHONY: push
push: build
	docker push ${IMAGE_NAME}:${TAG}

.PHONY: run
run:
	docker run --rm -d -p 5000:5000 ${IMAGE_NAME} ${run-arg}
