IMAGE_NAME := gabs79/udacity-frontendws

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
	docker run --rm -d -p 3000:3000 ${IMAGE_NAME} ${run-arg}
