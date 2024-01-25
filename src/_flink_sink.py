from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from src import _textractor
from pathlib import Path
import json


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
        data_stream = env.add_source(kafka_consumer)
        link_extract = data_stream.map(self.link_extractor, output_type=Types.STRING())
        link_extract.print()
        env.execute()
    
    def link_extractor(self, element):
        try:
            json_data = json.loads(element[5:])
            link = json_data['link']
            link_content = self.extract_news_text(link)
            json_data['content'] = link_content
            return str(json_data)
        except Exception as e:
            print(f"CRITICAL: Error processing element: {element}, error: {str(e)}")
        
    def extract_news_text(self, links):
        extractor = _textractor.TextExtractor()
        text = extractor.extract_text_from_link(links)
        extractor.close()
        lines = [line for line in text.split('\n') if not line.isupper()]
        final_text = '\n'.join(lines)
        return final_text


