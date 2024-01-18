import requests, json
import xml.etree.ElementTree as ET


class RSSFeedFetcher:

    def __init__(self, feed_url:str, parser:str = ET):
        self.feed_url = feed_url
        self.parser = parser

    def fetch_feed(self):
        feed_content = requests.get(self.feed_url)
        try:
            parsed_feed = self.parser.parse(feed_content.text)
        except OSError:
            parsed_feed = self.parser.fromstring(feed_content.text)
        return parsed_feed

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

