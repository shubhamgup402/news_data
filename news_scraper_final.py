import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time, random, re, os, csv, json
from langdetect import detect
from googletrans import Translator
from urllib.parse import urljoin, urlparse, urlunparse

translator = Translator()

# ================= UTILITIES =================

def is_english(text):
    try:
        return detect(text) == 'en'
    except:
        return False

def translate_to_english(text):
    try:
        return translator.translate(text, dest='en').text
    except:
        return text

def clean_url(url):
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))

def format_date(date):
    return date.strftime("%d/%m/%Y")

def _from_iso(s):
    """Robust ISO8601 -> datetime (handles Z)."""
    s = s.strip()
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _try_many_strptime(s, fmts):
    s = s.strip()
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None

def _strip_tz_words(s):
    # Remove common timezone words (IST, GMT, UTC) but keep offset if present
    return re.sub(r"\b(IST|GMT|UTC)\b", "", s, flags=re.I).strip()

def parse_relative_time(relative_text, base_date):
    """
    Convert Google relative/absolute label to datetime.
    If relative (hours/minutes/days/weeks), anchor to now.
    If absolute (e.g., 'Oct 10, 2017'), parse the date (time may be missing).
    """
    txt = relative_text.lower().strip()
    now = datetime.now()

    # Relative
    try:
        if "hour" in txt:
            n = int(re.search(r"(\d+)", txt).group(1))
            return now - timedelta(hours=n)
        if "minute" in txt:
            n = int(re.search(r"(\d+)", txt).group(1))
            return now - timedelta(minutes=n)
        if "day" in txt and "ago" in txt:
            n = int(re.search(r"(\d+)", txt).group(1))
            return now - timedelta(days=n)
        if "week" in txt:
            n = int(re.search(r"(\d+)", txt).group(1))
            return now - timedelta(weeks=n)
        if "yesterday" in txt:
            return now - timedelta(days=1)
    except Exception:
        pass

    # Absolute dates Google sometimes shows (no time)
    # Examples: "10 Oct 2017", "Oct 10, 2017", "October 10, 2017"
    abs_try = (
        _try_many_strptime(relative_text, ["%d %b %Y", "%b %d, %Y", "%B %d, %Y", "%d %B %Y"])
    )
    if abs_try:
        # Date-only -> keep date; we won't fake time here
        return abs_try

    # Fallback to base_date with current time to avoid midnight-looking zeros
    try:
        return datetime.combine(base_date.date(), now.time())
    except Exception:
        return now

# ---------------- FINANCIAL FILTER ----------------
finance_keywords = [
    "stock","share","market","nse","bse","sensex","nifty",
    "ipo","quarter","q1","q2","q3","q4","profit","loss",
    "earnings","dividend","revenue","forecast","sebi","investor",
    "fund","equity","valuation","bond","debt","merger","acquisition",
    "guidance","eps","rerating","brokerage","fpo","rights issue",
    "buyback","pledge","promoter","debenture", "buy","sell","hold","target price"
]

def is_financial_news(title, snippet):
    text = (title + " " + snippet).lower()
    return any(word in text for word in finance_keywords)

def is_strictly_relevant(title, stock_name, extra_keywords=None):
    """Company relevance; tune this list per your needs."""
    title_lower = title.lower()
    stock_lower = stock_name.lower()
    required_keywords = [
        stock_lower,
        "reliance industries", "ril", "mukesh ambani",
        "reliance jio", "reliance retail"
    ]
    if extra_keywords:
        required_keywords.extend([kw.lower() for kw in extra_keywords])
    return any(re.search(rf"\b{re.escape(kw)}\b", title_lower) for kw in required_keywords)

# ================= TIMESTAMP EXTRACTION FROM ARTICLE =================

META_NAME_KEYS = [
    "pubdate", "publish-date", "publishdate", "date", "dc.date", "dc.date.issued",
    "article:published_time", "article:modified_time", "parsely-pub-date"
]
META_PROP_KEYS = [
    "article:published_time", "article:modified_time", "og:updated_time", "og:pubdate"
]
ITEMPROP_KEYS = ["datePublished", "dateModified"]

VISIBLE_PATTERNS = [
    # "Published: Sunday, April 3, 2016, 19:10 IST"
    r"(?:Published|Updated)\s*:\s*[A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm)?\s*(?:ist|gmt|utc)?",
    # "Updated: September 3, 2016 7:11 PM IST"
    r"(?:Published|Updated)\s*:\s*[A-Za-z]+\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s*(?:am|pm)\s*(?:ist|gmt|utc)?",
    # "October 12, 2017 11:25 AM IST"
    r"[A-Za-z]+\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s*(?:am|pm)\s*(?:ist|gmt|utc)?",
    # "12 October 2017, 19:10 IST"
    r"\d{1,2}\s+[A-Za-z]+\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm)?\s*(?:ist|gmt|utc)?",
    # ISO-like embedded anywhere
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})"
]

STRPTIME_FORMATS = [
    "%A, %B %d, %Y, %H:%M",
    "%A, %B %d, %Y, %I:%M %p",
    "%B %d, %Y %I:%M %p",
    "%B %d, %Y, %I:%M %p",
    "%B %d, %Y, %H:%M",
    "%d %B %Y, %H:%M",
    "%d %B %Y %H:%M",
    "%d %b %Y, %H:%M",
    "%d %b %Y %H:%M",
    "%b %d, %Y %I:%M %p",
    "%Y-%m-%d %H:%M:%S",
]

def extract_article_timestamp(article_url, headers):
    """
    Fetch article page and extract publish datetime using:
    1) JSON-LD (datePublished/dateModified)
    2) meta tags (article:published_time, itemprop=datePublished, og:updated_time, etc.)
    3) <time datetime="...">
    4) Visible text patterns
    Returns datetime or None
    """
    try:
        resp = requests.get(article_url, headers=headers, timeout=12)
        if resp.status_code != 200 or not resp.text:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")

        # --- 1) JSON-LD ---
        for sc in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                data = json.loads(sc.string) if sc.string else None
            except Exception:
                # Some sites have invalid JSON with trailing commas; try a crude cleanup
                try:
                    cleaned = re.sub(r",\s*}", "}", sc.get_text())
                    cleaned = re.sub(r",\s*]", "]", cleaned)
                    data = json.loads(cleaned)
                except Exception:
                    data = None
            if not data:
                continue

            def pick_from(d):
                if not isinstance(d, dict):
                    return None
                # Prefer NewsArticle/Article
                if d.get("@type") in ("NewsArticle", "Article", "ReportageNewsArticle", "BlogPosting"):
                    for key in ("datePublished", "dateCreated", "uploadDate", "dateModified"):
                        if d.get(key):
                            dt = _from_iso(d[key])
                            if dt:
                                return dt
                            # Try forgiving formats
                            dt = _try_many_strptime(_strip_tz_words(d[key]), STRPTIME_FORMATS)
                            if dt:
                                return dt
                # Sometimes nested in graph
                if "@graph" in d and isinstance(d["@graph"], list):
                    for item in d["@graph"]:
                        dt = pick_from(item)
                        if dt:
                            return dt
                return None

            # data could be dict or list
            if isinstance(data, list):
                for item in data:
                    dt = pick_from(item)
                    if dt:
                        return dt
            else:
                dt = pick_from(data)
                if dt:
                    return dt

        # --- 2) Meta tags ---
        # property= / name= / itemprop=
        for prop in META_PROP_KEYS:
            tag = soup.find("meta", {"property": prop})
            if tag and tag.get("content"):
                s = tag["content"]
                dt = _from_iso(s) or _try_many_strptime(_strip_tz_words(s), STRPTIME_FORMATS)
                if dt: return dt

        for name in META_NAME_KEYS:
            tag = soup.find("meta", {"name": name})
            if tag and tag.get("content"):
                s = tag["content"]
                dt = _from_iso(s) or _try_many_strptime(_strip_tz_words(s), STRPTIME_FORMATS)
                if dt: return dt

        for itemp in ITEMPROP_KEYS:
            tag = soup.find("meta", {"itemprop": itemp})
            if tag and tag.get("content"):
                s = tag["content"]
                dt = _from_iso(s) or _try_many_strptime(_strip_tz_words(s), STRPTIME_FORMATS)
                if dt: return dt

        # --- 3) <time> tags ---
        for t in soup.find_all("time"):
            if t.get("datetime"):
                s = t["datetime"]
                dt = _from_iso(s) or _try_many_strptime(_strip_tz_words(s), STRPTIME_FORMATS)
                if dt: return dt
            # Sometimes time text holds "October 12, 2017 11:25 AM IST"
            s = t.get_text(" ", strip=True)
            if s:
                dt = _try_many_strptime(_strip_tz_words(s), STRPTIME_FORMATS)
                if dt: return dt

        # --- 4) Visible text patterns ---
        page_text = soup.get_text(" ", strip=True)
        for pat in VISIBLE_PATTERNS:
            m = re.search(pat, page_text, flags=re.I)
            if not m:
                continue
            s = _strip_tz_words(m.group(0))
            # Pull the date/time-ish chunk from the matched string
            # Try ISO first
            iso = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})", s)
            if iso:
                dt = _from_iso(iso.group(0))
                if dt: return dt
            # Else try a set of common strptime formats
            dt = _try_many_strptime(s, STRPTIME_FORMATS)
            if dt: return dt

        return None

    except Exception:
        return None

# ================= SCRAPER =================

def fetch_news_for_date(stock_name, date, headers, extra_keywords=None):
    articles, start, retries = [], 0, 0
    formatted_date = format_date(date)

    while True:
        url = (
            f"https://www.google.com/search?q={stock_name}+finance+OR+stock+OR+business+OR+market+news"
            f"&tbm=nws&tbs=cdr:1,cd_min:{formatted_date},cd_max:{formatted_date}&hl=en&gl=in&start={start}"
        )

        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 429:
                wait = random.randint(30, 90)
                print(f"429 Too Many Requests. Sleeping {wait}s...")
                time.sleep(wait)
                continue
            if response.status_code != 200:
                retries += 1
                if retries > 3:
                    break
                time.sleep(2 ** retries)
                continue
        except requests.RequestException:
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        results = soup.find_all('div', class_='SoaBEf')
        if not results:
            break

        for result in results:
            title_tag = result.find('div', class_='n0jPhd ynAwRc MBeuO nDgy9d')
            snippet_tag = result.find('div', class_='GI74Re nDgy9d')
            link_tag = result.find('a')
            time_tag = result.find('span', class_='WG9SHc') or result.find('time')

            if not title_tag or not link_tag:
                continue

            title = title_tag.text.strip()
            snippet = snippet_tag.text.strip() if snippet_tag else "No summary"
            link = clean_url(urljoin("https://www.google.com", link_tag.get('href')))

            # 1) Try to extract the REAL published time from the article page
            article_time = extract_article_timestamp(link, headers)

            # 2) Fallback to Google's relative/absolute label
            if not article_time and time_tag:
                rel = (time_tag.get_text() or "").strip()
                article_time = parse_relative_time(rel, date)

            # 3) Final fallback: use the loop date with current time (avoid 00:00:00 look)
            if not article_time:
                now = datetime.now()
                article_time = datetime.combine(date.date(), now.time())

            # Translate if needed
            if not is_english(title):
                title = translate_to_english(title)
            if not is_english(snippet):
                snippet = translate_to_english(snippet)

            if is_strictly_relevant(title, stock_name, extra_keywords) and is_financial_news(title, snippet):
                articles.append({
                    "timestamp": article_time.strftime("%d-%m-%Y %H:%M:%S"),
                    "title": title,
                    "summary": snippet,
                    "url": link
                })

        start += 10
        time.sleep(random.uniform(5, 15))
        if start % 30 == 0:
            print(" Reached 30 results, cooldown...")
            time.sleep(random.uniform(60, 120))

    return articles

def save_to_csv(news_list, filename="news_results.csv"):
    file_exists = os.path.isfile(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "title", "summary", "url"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(news_list)
    print(f"  Saved {len(news_list)} articles to {filename}")

def scrape_news_day_by_day(stock_name, start_date, end_date=None, extra_keywords=None):
    headers = {'User-Agent': random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/118.0"
    ])}
    current_date = start_date
    if not end_date:
        end_date = datetime.today()

    print(f"\n Fetching financial news for '{stock_name}' from {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}...\n")

    while current_date <= end_date:
        print(f" {current_date.strftime('%d %b %Y')}...")
        day_articles = fetch_news_for_date(stock_name, current_date, headers, extra_keywords)
        # Dedup by title
        unique_news = {item['title']: item for item in day_articles}.values()
        if unique_news:
            save_to_csv(unique_news)
        current_date += timedelta(days=1)

# ================= MAIN =================
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
