# main.py (Module 6: Pipeline Orchestration)
# This script orchestrates the entire scraping and data storage pipeline.
# It loads configurations, initializes the web scraper, and stores data in MongoDB.
# Logs are maintained for debugging and monitoring purposes.

import sys
import os
import time
from datetime import datetime
import configparser
import logging
import pandas as pd
from scraper import *
from mongodb import *
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Configure logging
LOG_FILE = "pipeline.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",  # Overwrite each run
)

def load_config(config_file):
    """
    Load configuration from the config file.
    """
    config = configparser.ConfigParser()
    if not os.path.exists(config_file):
        logging.error(f"Configuration file '{config_file}' not found.")
        sys.exit(1)
    config.read(config_file)
    return config

def scrape_and_store_data(config):
    """
    Orchestrates the scraping and data storage pipeline.
    """
    try:
        logging.info("Pipeline started.")
        
        # Extract configuration values
        google_news_url = config["WEBSCRAPING"]["google_news_url"]
        top_stories_id = config["WEBSCRAPING"]["top_stories_id"]
        element_class_name_1 = config["WEBSCRAPING"]["element_class_name_1"]
        element_class_name_2 = config["WEBSCRAPING"]["element_class_name_2"]
        thumbnail_class_name = config["WEBSCRAPING"]["thumbnail_class_name"]
        article_class_name_1 = config["WEBSCRAPING"]["article_class_name_1"]
        article_class_name_2 = config["WEBSCRAPING"]["article_class_name_2"]
        article_class_url = config["WEBSCRAPING"]["article_class_url"]
        article_class_time = config["WEBSCRAPING"]["article_class_time"]
        
        # Initialize WebDriver
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)

        try:
            logging.info(f"Loading Google News homepage: {google_news_url}")
            driver.get(google_news_url)
            time.sleep(3)

            news_data = []  # Store scraped data

            logging.info("Scraping homepage...")
            fetch_news_details(driver, element_class_name_1, article_class_name_1, thumbnail_class_name, article_class_url, article_class_time, news_data)
            fetch_news_details(driver, element_class_name_2, article_class_name_2, thumbnail_class_name, article_class_url, article_class_time, news_data)
            
            logging.info("Extracting 'Top Stories' URL...")
            top_stories_url = retrieve_top_stories_url(driver, top_stories_id)
            if top_stories_url:
                driver.get(top_stories_url)
                time.sleep(3)
                logging.info("Scraping 'Top Stories' page...")
                fetch_news_details(driver, element_class_name_1, article_class_name_1, thumbnail_class_name, article_class_url, article_class_time, news_data)
            else:
                logging.warning("Could not find 'Top Stories' URL.")
            
            driver.quit()
            
            df = pd.DataFrame(news_data)
            df.to_csv("news_data.csv", index=False)
            logging.info("Scraping completed. Data saved to 'news_data.csv'.")

            store_data_in_mongodb(df)
            logging.info("Data inserted into MongoDB.")

        except Exception as e:
            logging.error(f"Error during scraping or data insertion: {str(e)}")
            driver.quit()
            raise

    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    config = load_config("config.ini")
    scrape_and_store_data(config)
    logging.info("Pipeline completed successfully.")
