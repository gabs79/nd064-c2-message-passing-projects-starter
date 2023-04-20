> diagrams. used lucidchart:
	https://lucid.app/lucidchart/295056dc-ccbb-465e-812f-fea8bfd43011/edit?invitationId=inv_635d1a5b-615b-495f-95e0-b8ba3e4acea4&page=0_0#
> installed helm
https://helm.sh/docs/intro/install/
> installed kafka chart: https://github.com/bitnami/charts/tree/main/bitnami/kafka
	
	helm repo add bitnami-repo https://charts.bitnami.com/bitnami
helm install kafka-release bitnami-repo/kafka
	output:
		can be accessed by consumer via port 9092 on following DNS
			kafka-release-0.kafka-release-headless.default.svc.cluster.local:9092
		to create pod that you can use as a Kafka client run:
			kubectl run kafka-release-client --restart='Never' --image docker.io/bitnami/kafka:3.4.0-debian-11-r15 --namespace default --command -- sleep infinity
			
			kubectl exec --tty -i kafka-release-client --namespace default -- bash
			
			PRODUCER:
				kafka-console-producer.sh --broker-list kafka-release-0.kafka-release-headless.default.svc.cluster.local:9092
				
			CONSUMER:
				kafka-console-consumer.sh --bootstrap-server kafka-release.default.svc.cluster.local:9092 --topic test --from-begenning