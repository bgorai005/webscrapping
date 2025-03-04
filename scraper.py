# scraper.py (Modules 1, 2, 3: Web Scraping)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
from datetime import datetime

def fetch_news_details(driver, element_class_name, article_class_name, thumbnail_class_name, article_class_url, article_class_time, news_data):
    """Scrapes image and article details."""
    try:
        articles = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, element_class_name)))
        for article in articles:
            try:
                image_element = article.find_element(By.CSS_SELECTOR, thumbnail_class_name)
                image_url = image_element.get_attribute("src")
                headline = article.find_element(By.CSS_SELECTOR, article_class_name).text
                article_url = article.find_element(By.CSS_SELECTOR, article_class_url).get_attribute("href")
                published_time = article.find_element(By.CSS_SELECTOR, article_class_time).text
                
                news_data.append({
                    "image_data": requests.get(image_url).content,
                    "headlines": headline,
                    "image_url": image_url,
                    "article_url": article_url,
                    "scrap_timestamp": datetime.now(),
                    "published_time": published_time,
                })
            except Exception:
                continue
    except Exception:
        return

def retrieve_top_stories_url(driver, url_ID):
    """Finds the 'Top Stories' URL dynamically."""
    try:
        return driver.find_element(By.ID, url_ID).get_attribute("href")
    except Exception:
        return None