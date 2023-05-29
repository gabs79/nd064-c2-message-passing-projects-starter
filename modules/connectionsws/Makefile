IMAGE_NAME := gabs79/udacity-connectionsws
TAG := v1.0.1
DEPLOYMENT := /vagrant/deployment/udaconnect-connection.yaml

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
	docker run --rm -d ${IMAGE_NAME}:${TAG} ${run-arg}

.PHONY: delete
delete:
	kubectl delete -f ${DEPLOYMENT}

.PHONY: redeploy
redeploy: delete deploy

.PHONY: deploy
deploy: push
	kubectl apply -f ${DEPLOYMENT}
