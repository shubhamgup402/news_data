import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time, random, re
from langdetect import detect
from googletrans import Translator
import csv
from urllib.parse import urljoin, urlparse, urlunparse
import os

translator = Translator()

# ========== UTILITIES ==========

def is_english(text):
    """Detect if text is English"""
    try:
        return detect(text) == 'en'
    except:
        return False

def translate_to_english(text):
    """Translate non-English text to English"""
    try:
        return translator.translate(text, dest='en').text
    except:
        return text

def clean_url(url):
    """Remove tracking parameters"""
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))

def format_date(date):
    return date.strftime("%d/%m/%Y")

def is_strictly_relevant(title, stock_name, extra_keywords=None):
    """Check if article is about the stock/company"""
    title_lower = title.lower()
    stock_lower = stock_name.lower()

    required_keywords = [
        stock_lower,
        "reliance industries",
        "ril",
        "mukesh ambani",
        "reliance jio",
        "reliance retail"
    ]
    if extra_keywords:
        required_keywords.extend([kw.lower() for kw in extra_keywords])

    return any(re.search(rf"\b{re.escape(kw)}\b", title_lower) for kw in required_keywords)

# ========== SCRAPER ==========

def fetch_news_for_date(stock_name, date, headers, extra_keywords=None):
    """Fetch Google News for a single day"""
    articles, start, retries = [], 0, 0
    formatted_date = format_date(date)

    while True:
        url = (
            f"https://www.google.com/search?q={stock_name}+news"
            f"&tbm=nws&tbs=cdr:1,cd_min:{formatted_date},cd_max:{formatted_date}&hl=en&start={start}"
        )

        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f" Error {response.status_code} on {formatted_date}")
                retries += 1
                if retries > 3: break
                time.sleep(2 ** retries)
                continue
        except requests.RequestException as e:
            print(f" Network error: {e}")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        results = soup.find_all('div', class_='SoaBEf')
        if not results:
            break

        for result in results:
            title_tag = result.find('div', class_='n0jPhd ynAwRc MBeuO nDgy9d')
            snippet_tag = result.find('div', class_='GI74Re nDgy9d')
            link_tag = result.find('a')

            if not title_tag or not link_tag:
                continue

            title = title_tag.text.strip()
            summary = snippet_tag.text.strip() if snippet_tag else "No summary"
            link = urljoin("https://www.google.com", link_tag.get('href'))
            link = clean_url(link)

            # Translate if needed
            if not is_english(title):
                title = translate_to_english(title)
            if not is_english(summary):
                summary = translate_to_english(summary)

            if is_strictly_relevant(title, stock_name, extra_keywords):
                articles.append({
                    "date": formatted_date,
                    "title": title,
                    "summary": summary,
                    "url": link
                })

        start += 10
        time.sleep(random.uniform(1, 3))  # Random delay to avoid blocking

    return articles

def save_to_csv(news_list, filename="news_results.csv"):
    """Append daily results into CSV"""
    file_exists = os.path.isfile(filename)

    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "title", "summary", "url"])
        if not file_exists:  # Write header only once
            writer.writeheader()
        writer.writerows(news_list)

    print(f"  Saved {len(news_list)} articles to {filename}")

def scrape_news_day_by_day(stock_name, start_date, end_date=None, extra_keywords=None):
    headers = {
        'User-Agent': random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/118.0"
        ])
    }

    current_date = start_date
    if not end_date:
        end_date = datetime.today()

    print(f"\n Fetching news for '{stock_name}' from {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}...\n")

    while current_date <= end_date:
        print(f" {current_date.strftime('%d %b %Y')}...")
        day_articles = fetch_news_for_date(stock_name, current_date, headers, extra_keywords)

        # Deduplicate same-day by title
        unique_news = {item['title']: item for item in day_articles}.values()

        if unique_news:
            save_to_csv(unique_news)

        current_date += timedelta(days=1)


# ========== MAIN ==========
if __name__ == "__main__":
    stock_name = input("Enter stock/company name (e.g., Reliance): ").strip()
    date_input = input("Enter start date (dd-mm-yyyy): ").strip()
    end_date_input = input("Enter end date (dd-mm-yyyy) [optional]: ").strip()

    try:
        start_date = datetime.strptime(date_input, "%d-%m-%Y")
        end_date = datetime.strptime(end_date_input, "%d-%m-%Y") if end_date_input else None
    except ValueError:
        print(" Invalid date format.")
        exit()

    scrape_news_day_by_day(stock_name, start_date, end_date)
