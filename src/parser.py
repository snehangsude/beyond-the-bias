import requests, json, bs4, typing
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod

class FeedParser(ABC):
    @abstractmethod
    def parse(self, content: str) -> typing.Any:
        pass

class XMLFeedParser(FeedParser):
    def parse(self, content: str) -> ET.Element:
        return ET.fromstring(content)


class HTMLFeedParser(FeedParser):
    def parse(self, content: str) -> bs4.BeautifulSoup:
        return bs4.BeautifulSoup(content, "html.parser")


class RSSFeedFetcher:

    def __init__(self, feed_url:str, parser_type:str = FeedParser):
        self.feed_url = feed_url
        self.parser = parser_type

    def fetch_feed(self):
        feed_content = requests.get(self.feed_url)
        return self.parser.parse(feed_content.text)

class FeedCollector:

    def __init__(self):
        self.feed_arr = []

    def process_feed(self, data):

        items = data.findall('channel/item')
        creator = data.find("channel/copyright").text
        for item in items:
            feed_struct = {} 
            title = item.find('title').text 
            link = item.find('link').text 
            description = item.find('description').text
            pubDate = item.find('pubDate').text


            feed_struct['title'] = title
            feed_struct['description'] = description
            feed_struct['link'] = link
            feed_struct['pubdate'] = pubDate
            feed_struct['source'] = creator
            
            yield json.dumps(feed_struct)

