from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pathlib import Path


class FlinkProcessor:
    def __init__(self, config: dict, topic:str, jar_path:str):
        self.config = config
        self.topic = topic
        self.jar_path = Path(jar_path).as_uri()

    def create_data_stream(self):
        env = StreamExecutionEnvironment.get_execution_environment()
        env.add_jars(self.jar_path)
        kafka_consumer = FlinkKafkaConsumer(
            topics=[self.topic],
            properties = self.config,
            deserialization_schema=SimpleStringSchema()
        )

        kafka_consumer.set_start_from_earliest()
        env.add_source(kafka_consumer).print()
        env.execute()

    def preprocess_data(self, data_stream):
        return (data_stream)

    def filter_messages(self, message):
        return

    def extract_data(self, message):
        return


