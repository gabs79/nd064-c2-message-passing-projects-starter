IMAGE_NAME := gabs79/udacity-locationconsumerws
TAG := v1.0.1
DEPLOYMENT := /vagrant/deployment/udaconnect-location.yaml

.PHONY: usage
usage:
	echo 'Usage: [build] [push] [run run-arg=<>]'

.PHONY: build
build:
	docker build -t ${IMAGE_NAME} .

.PHONY: push
push: build
	docker push ${IMAGE_NAME}

.PHONY: run
run:
	docker run --rm -d ${IMAGE_NAME} ${run-arg}

.PHONY: delete
delete:
	kubectl delete -f ${DEPLOYMENT}

.PHONY: redeploy
redeploy: delete deploy

.PHONY: deploy
deploy: push
	kubectl apply -f ${DEPLOYMENT}
