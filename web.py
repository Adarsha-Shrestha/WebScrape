from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import logging
from datetime import datetime
import os
from urllib3.exceptions import ReadTimeoutError
from requests.exceptions import RequestException
from tenacity import retry, stop_after_attempt, wait_exponential

class NewsScraperSetopati:
    def __init__(self, headless=True):
        """Initialize the scraper with Chrome webdriver and setup logging"""
        # Setup logging
        logging.basicConfig(
            filename=f'setopati_scraper_{datetime.now().strftime("%Y%m%d")}.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Initialize webdriver with options
        self.options = webdriver.ChromeOptions()
        
        if headless:
            self.options.add_argument('--headless=new')
        
        # Suppress unnecessary logging
        self.options.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.options.add_argument('--log-level=3')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        
        # Add timeout settings
        self.options.add_argument('--timeout=300000')  # 5 minute timeout
        self.options.add_argument('--page-load-timeout=300000')
        
        # Network settings
        self.options.add_argument('--disable-network-throttling')
        self.options.add_argument('--disable-application-cache')
        
        # Suppress DevTools listening message
        os.environ['PYTHONWARNINGS'] = 'ignore'
        
        self.setup_driver()

    def setup_driver(self):
        """Setup WebDriver with retry mechanism"""
        max_attempts = 3
        attempt = 0
        
        while attempt < max_attempts:
            try:
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(
                    service=service,
                    options=self.options
                )
                self.driver.set_page_load_timeout(300)  # 5 minutes
                self.wait = WebDriverWait(self.driver, 30)  # Increased wait time
                break
            except Exception as e:
                attempt += 1
                logging.error(f"Attempt {attempt} failed to initialize driver: {str(e)}")
                if attempt == max_attempts:
                    raise Exception("Failed to initialize WebDriver after multiple attempts")
                time.sleep(5)  # Wait before retrying

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def get_article_links(self, url, max_scrolls=5):
        """
        Scroll through the page and collect article links with retry mechanism
        """
        article_links = set()
        try:
            self.driver.get(url)
            
            for scroll in range(max_scrolls):
                try:
                    # Scroll with error handling
                    self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);"
                    )
                    
                    # Wait for new content with explicit wait
                    self.wait.until(lambda driver: len(driver.find_elements(
                        By.CSS_SELECTOR, 'a.title')) > len(article_links)
                    )
                    
                    # Find all article links
                    links = self.driver.find_elements(By.CSS_SELECTOR, 'a.title')
                    for link in links:
                        href = link.get_attribute('href')
                        if href and 'setopati.com' in href:
                            article_links.add(href)
                            
                    time.sleep(2)  # Controlled delay between scrolls
                    
                except TimeoutException:
                    logging.warning(f"Timeout during scroll {scroll + 1}, continuing...")
                    continue
                    
            logging.info(f"Collected {len(article_links)} article links")
            return list(article_links)
            
        except WebDriverException as e:
            logging.error(f"WebDriver error while collecting links: {str(e)}")
            self.restart_driver()
            raise  # Retry through decorator
        except Exception as e:
            logging.error(f"Unexpected error collecting links: {str(e)}")
            raise

    def restart_driver(self):
        """Restart the WebDriver if it encounters issues"""
        try:
            self.driver.quit()
        except:
            pass
        self.setup_driver()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def scrape_article(self, url):
        """
        Scrape individual article content with retry mechanism
        """
        try:
            self.driver.get(url)
            
            # Wait for main content with increased timeout
            content_element = self.wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, 'news-content'))
            )
            
            # Get article data with explicit waits
            article_data = {
                'url': url,
                'title': self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.news-big-title'))
                ).text,
                'date': self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'span.news-time'))
                ).text,
                'content': content_element.text,
                'category': self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.breadcrumb'))
                ).text
            }
            
            # Try to get author if available
            try:
                article_data['author'] = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.author-name'))
                ).text
            except TimeoutException:
                article_data['author'] = 'Unknown'
                
            logging.info(f"Successfully scraped article: {url}")
            return article_data
            
        except TimeoutException as e:
            logging.error(f"Timeout while scraping article {url}: {str(e)}")
            raise  # Retry through decorator
        except WebDriverException as e:
            logging.error(f"WebDriver error while scraping {url}: {str(e)}")
            self.restart_driver()
            raise  # Retry through decorator
        except Exception as e:
            logging.error(f"Unexpected error scraping {url}: {str(e)}")
            raise

    def scrape_website(self, base_url, max_articles=50):
        """
        Main function to scrape the website with error recovery
        """
        try:
            # Get article links with retry
            article_links = self.get_article_links(base_url)[:max_articles]
            
            if not article_links:
                logging.error("No article links found")
                return pd.DataFrame()
            
            # Scrape articles
            articles_data = []
            for url in article_links:
                try:
                    article_data = self.scrape_article(url)
                    if article_data:
                        articles_data.append(article_data)
                    time.sleep(2)  # Increased delay between requests
                except Exception as e:
                    logging.error(f"Failed to scrape article {url}: {str(e)}")
                    continue
                
            # Create DataFrame
            df = pd.DataFrame(articles_data)
            
            if not df.empty:
                # Save to CSV with error handling
                filename = f'setopati_articles_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
                try:
                    df.to_csv(filename, index=False, encoding='utf-8-sig')
                    logging.info(f"Successfully saved {len(df)} articles to {filename}")
                except Exception as e:
                    logging.error(f"Error saving to CSV: {str(e)}")
            
            return df
            
        except Exception as e:
            logging.error(f"Error in main scraping process: {str(e)}")
            return pd.DataFrame()
            
        finally:
            try:
                self.driver.quit()
            except:
                pass

# Example usage with error handling
if __name__ == "__main__":
    try:
        scraper = NewsScraperSetopati(headless=True)
        df = scraper.scrape_website(
            base_url="https://www.setopati.com",
            max_articles=20
        )
        print(f"Scraped {len(df)} articles successfully!")
    except Exception as e:
        print(f"Scraping failed: {str(e)}")