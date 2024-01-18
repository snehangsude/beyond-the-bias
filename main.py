import sys, os, datetime
from configparser import ConfigParser
from src import rss
from src import _kafka_stream
from src import _flink_sink

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pathlib import Path


config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser()
config.read(f"{config_path[0]}/beyond-the-bias/config.ini")
config_data= dict(config["default"])
config_data.update(config['consumer'])


robot = rss.RSSFeedFetcher(feed_url="https://rss.nytimes.com/services/xml/rss/nyt/World.xml")
data = robot.fetch_feed()

fc = rss.FeedCollector()
ks = _kafka_stream.PushKafkaEvents(config=config_data)
kafka_topic = 'rss-news'
key_generator = str(datetime.date.today())

for json_string in fc.process_feed(data):
        ks.produce_message(
                topic=kafka_topic,
                key=key_generator,
                value=json_string
        )
        ks.flush()




processor = _flink_sink.FlinkProcessor(config_data, kafka_topic, jar_path="/home/cloudcraftz-11/Public/Python/Personal/beyond-the-bias/src/flink-sql-connector-kafka-1.16.0.jar")
data_stream = processor.create_data_stream()
# print(data_stream)

# kc = kafka_stream.ConsumeKafkaEvents(config_data, topic=kafka_topic)
# kc.subscribe()
# kc.poll_messages()