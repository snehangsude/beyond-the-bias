from bs4 import BeautifulSoup as bs
from selenium import webdriver
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class TextExtractor:
    def __init__(self):
        options = Options()
        options.add_argument('-headless')
        options.set_preference("permissions.default.image", 2)
        options.set_preference("javascript.enabled", False)
        options.add_argument("--start-maximized")
        self.driver = webdriver.Firefox(service=Service(executable_path=GeckoDriverManager().install()), options=options)

    def extract_text_from_link(self, url):
        try:
            self.driver.get(url)
            text_element = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "story")))
            text = text_element.text
            return text
        except Exception as e:
            print(f"Error extracting text from {url}: {e}")
            return None

    def close(self):
        self.driver.quit()
