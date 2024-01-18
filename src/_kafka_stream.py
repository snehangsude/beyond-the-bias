import json
from avro import schema
from confluent_kafka.avro import AvroProducer
from confluent_kafka.error import KafkaException


class PushKafkaEvents:

    def __init__(self, config:dict, value_schema:str, key_schema:str):

        self.avro_value_schema = schema.Parse(value_schema)
        self.avro_key_schema = schema.Parse(key_schema)
        self.producer = AvroProducer(
            config, 
            default_key_schema=self.avro_key_schema, 
            default_value_schema=self.avro_value_schema
        ) 
    
    def produce_message(self, topic:str, key:dict, value:str):
        try:
            avro_value = json.loads(value)
            self.producer.produce(
                topic=topic,
                key=key if key else None,
                value=avro_value if avro_value else None,
            )
            print(f"SUCCESS: {key}: {avro_value}")
        except KafkaException as e:
            print(f'CRITICAL: During message production: {e}')

    def flush(self):
        self.producer.poll(100000)
        self.producer.flush()

