import sys, os, datetime, re
from configparser import ConfigParser
from src import parser
from src import _kafka_stream
from src import _flink_sink

# Confiuguration INIT
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser()
config.read(f"{config_path[0]}/beyond-the-bias/config.ini")

# Kafka Configurations
kafka_server = dict(config["KAFKA"])
kafka_server.pop("schema.registry.url")
kafka_registry = config.get('KAFKA', 'schema.registry.url')
kafka_topic = config.get('KAKFA_SCHEMA', 'topic')
kafka_config = dict(config['KAFKA_ADDONS'])
for key, value in kafka_config.items():
    if value.startswith('{') and value.endswith('}'):
        kafka_config[key] = eval(value)
kafka_config.update(kafka_server)

# Schema Registry Configurations
value_schema = config.get("KAKFA_SCHEMA", "rss_value_schema")
key_schema = config.get("KAKFA_SCHEMA", "rss_key_schema")

url = "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml"
pattern = r"rss\.(.*?)\.com"

# Scraping various news feeds
xml_parser = parser.XMLFeedParser()
robot = parser.RSSFeedFetcher(feed_url=url, parser_type=xml_parser)
data = robot.fetch_feed()

# Collect data from scraped feeds
fc = parser.FeedCollector()
ks = _kafka_stream.PushKafkaEvents(
        config=kafka_config, 
        topic=kafka_topic,
        registry_url=kafka_registry,
        value_schema=value_schema, 
        key_schema=key_schema
        )

key_generator = {"time": str(datetime.datetime.now()), "publication": re.search(pattern, url).group(1)}
for json_string in fc.process_feed(data):
        ks.produce_message(
                topic=kafka_topic,
                key=key_generator,
                value=json_string.strip()
        )
        ks.flush()
        # break

# TODO 1: Push to new FILE

processor = _flink_sink.FlinkProcessor(kafka_server, kafka_topic, jar_path=config.get('FLINK', 'jar_path'))
data_stream = processor.create_data_stream()
