import json
from confluent_kafka import Producer
from confluent_kafka.error import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext


class PushKafkaEvents:

    def __init__(self, config:dict, topic:str, registry_url:str, value_schema:str, key_schema:str, ):
        
        self.topic = topic
        self.schemaReg_client = SchemaRegistryClient({'url': registry_url})
        self.value_schema = Schema(
            schema_str=value_schema, 
            schema_type="JSON"
        )
        self.value_schema_id = self.schemaReg_client.register_schema(
            subject_name=self.topic,
            schema=self.value_schema
        )
        self.producer = Producer(config) 
        self.json_value_serializer = JSONSerializer(self.value_schema, self.schemaReg_client)
    

    def produce_message(self, topic:str, key:dict, value:str):
        
            
        ctx = SerializationContext(self.topic, "value")
        serialized_value = self.json_value_serializer(json.loads(value), ctx)
        try:
            self.producer.produce(
                topic=topic,
                key=str(key).encode("utf-8"),
                value=serialized_value,
            )
            print(f"SUCCESS: {str(key).encode('utf-8')}: {(str(value).encode('utf-8'))}\n")
        except KafkaException as e:
            print(f'CRITICAL: During message production: {e}')

    def flush(self):
        self.producer.poll(100000)
        self.producer.flush()

