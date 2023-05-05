from kafka import KafkaConsumer

TOPIC_NAME = 'items'

consumer = KafkaConsumer(TOPIC_NAME)
for message in consumer:
    #todo: recieve location, store location in un_process_location table/area/DB
    #every 5 min (update timer accordingly), another process calcualtes Connections
    #     and updates DB with connections table (as opposed to calculate per query ar runtime)
    print (message)

def create_kafka_consumer(topic_name:str):
    return KafkaConsumer(topic_name)

class Consumer():
    def __init__(self, kafka_consumer=create_kafka_consumer()) -> None:
        pass

    def consumer(self) -> None:
        pass

