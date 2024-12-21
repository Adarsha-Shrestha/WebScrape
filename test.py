from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import requests

# Configure Selenium WebDriver
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    service = Service('chromedriver.exe')  # Adjust path to your chromedriver
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

# Scrape articles
def scrape_articles(base_url,news_url, max_articles=2):
    driver = setup_driver()
    driver.get(news_url)
    all_articles = []
    
    try:
        time.sleep(2)  # Allow page to load
        soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Extract article title, teaser, and link
        articles = soup.find_all('div', class_='teaser offset', limit=max_articles)  # Adjust based on the website's structure
        for article in articles:
            try:
                title_tag = article.find('a')  # Get the title and link
                teaser = article.find('p').text.strip()  # Get the teaser text
                link = title_tag['href']
                    
                if link.startswith('/'):  # Handle relative links
                    link = base_url + link
                    print(link)
                    
                full_content = scrape_article_content(driver,link)  # Fetch full content
                all_articles.append({
                    'title': title_tag.text.strip(),
                    'teaser': teaser,
                    'link': link,
                    'content': full_content
                })
            except Exception as e:
                print(f"Error scraping article: {e}")
            
            # # Handle pagination (if applicable)
            # try:
            #     next_button = driver.find_element(By.CLASS_NAME, 'next')  # Replace with actual button class
            #     next_button.click()
            #     time.sleep(2)
            # except Exception as e:
            #     print(f"No more pages or pagination error: {e}")
            #     break
    finally:
        driver.quit()
    
    return all_articles

# Scrape full content of individual article
def scrape_article_content(driver,url):
    
    driver.get(url)    
    try:
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Extract article content
        article_section = soup.find('div', class_='description current-news-block portrait')
        if not article_section:
            return None

        paragraphs = article_section.find_all('p') # Extract all <p> tags
        full_content = ' '.join([p.text.strip() for p in paragraphs])  # Combine paragraph text
        return full_content
    except Exception as e:
        print(f"Error loading article content: {e}")
        return None

# Save articles to a file
def save_articles(articles, filename='articles.json'):
    import json
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(articles, f, ensure_ascii=False, indent=4)

# Main Execution
if __name__ == "__main__":
    base_url = "https://ekantipur.com"  # Adjust if needed
    news_url = f"{base_url}/news"
    articles = scrape_articles(base_url,news_url)
    save_articles(articles)
    print(f"Scraped {len(articles)} articles.")
