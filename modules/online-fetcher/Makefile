IMAGE_NAME := gabs79/udacity/onlinefetcherws

.PHONY: usage
usage:
	echo 'Usage: [build] [push] [run run-arg=<>]'

.PHONY: build-proto
build-proto:
	python -m venv .venv
	source .venv/bin/activate
	pip install --requirement requirements-build.txt
	grpc_tools.protoc -I./ --python_out=./ --grpc_python_out=./ person.proto
	deactivate

.PHONY: build
build:
	docker build -t ${IMAGE_NAME} .

.PHONY: push
push:
	docker push ${IMAGE_NAME}

.PHONY: run
run:
	docker run --rm -d -p 5005:5005 ${IMAGE_NAME} ${run-arg}
