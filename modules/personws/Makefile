IMAGE_NAME := gabs79/udacity-personproducerws
TAG := v1.0.1
DEPLOYMENT := /vagrant/deployment/udaconnect-person.yaml

.PHONY: usage
usage:
	echo 'Usage: [build] [push] [run run-arg=<>]'

.PHONY: build-proto
build-proto:
	python -m venv .venv
	source .venv/bin/activate
	pip install --requirement requirements-build.txt
	python -m grpc_tools.protoc -I./ --python_out=./ --grpc_python_out=./ person.proto
	deactivate

.PHONY: build
build:
	docker build -t ${IMAGE_NAME}:${TAG} .

.PHONY: push
push: build
	docker push ${IMAGE_NAME}:${TAG}

.PHONY: run
run:
	docker run --rm -d -p 5005:5005 ${IMAGE_NAME} ${run-arg}

.PHONY: delete
delete:
	kubectl delete -f ${DEPLOYMENT}

.PHONY: redeploy
redeploy: delete deploy

.PHONY: deploy
deploy: push
	kubectl apply -f ${DEPLOYMENT}
