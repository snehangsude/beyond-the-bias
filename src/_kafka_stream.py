from typing import Callable
from confluent_kafka import Producer


class PushKafkaEvents:

    def __init__(self, config:dict, delivery_callback:Callable=None):
        self.producer = Producer(config)
        self.delivery_callback = delivery_callback

    def delivery_report(self, err, msg):
        
        if self.delivery_callback:
            self.delivery_callback(err, msg)
        else:
            if err:
                print(f'ERROR: Message failed delivery: {err}')
            else:
                print(f"SUCCESS: Produced event to topic- {msg.topic()}: key = {msg.key().decode('utf-8')} value = {msg.value().decode('utf-8')}")  
    
    def produce_message(self, topic, key, value):
        try:
            self.producer.produce(
                topic,
                key=key.encode('utf-8') if key else None,
                value=value.encode('utf-8') if value else None,
                callback=self.delivery_report
            )
        except KafkaException as e:
            print(f'CRITICAL: During message production: {e}')

    def flush(self):
        self.producer.poll(100000)
        self.producer.flush()

