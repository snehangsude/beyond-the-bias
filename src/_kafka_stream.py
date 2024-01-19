import json
# from jsonschema import validate
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
from confluent_kafka.error import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.json_schema import JSONSerializer, JSONDeserializer
from confluent_kafka.serialization import SerializationContext


class PushKafkaEvents:

    def __init__(self, config:dict, topic:str, registry_url:str, value_schema:str, key_schema:str, ):

        self.schemaReg_client = SchemaRegistryClient({'url': registry_url})
        self.topic = topic
        self.value_schema = Schema(schema_str=value_schema, schema_type="JSON")
        self.value_schema_id = self.schemaReg_client.register_schema(
            subject_name=self.topic,
            schema=self.value_schema)
        self.producer = Producer(config) 
        self.json_value_serializer = JSONSerializer(self.value_schema, self.schemaReg_client)
    

    def produce_message(self, topic:str, key:dict, value:str):
        try:
            
            ctx = SerializationContext(self.topic, "value")
            serialized_value = self.json_value_serializer(json.loads(value), ctx)
            self.producer.produce(
                topic=topic,
                key=str(key).encode("utf-8"),
                value=serialized_value,
            )
            print(f"SUCCESS: {str(key).encode('utf-8')}: {serialized_value}\n")
        except KafkaException as e:
            print(f'CRITICAL: During message production: {e}')

    def flush(self):
        self.producer.poll(100000)
        self.producer.flush()

