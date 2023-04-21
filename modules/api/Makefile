IMAGE_NAME := gabs79/udacity/gatewayws

.PHONY: usage
usage:
	echo 'Usage: [build] [push] [run run-arg=<>]'

.PHONY: run-test
build-proto:
	python -m venv .venv
	source .venv/bin/activate
	pip install --requirement requirements.txt
	flask run --host 0.0.0.0

.PHONY: build
build:
	docker build -t ${IMAGE_NAME} .

.PHONY: push
push:
	docker push ${IMAGE_NAME}

.PHONY: run
run:
	docker run --rm -d -p 5000:5000 ${IMAGE_NAME} ${run-arg}
