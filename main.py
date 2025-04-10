from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Optional
from googlesearch import search
import requests
from bs4 import BeautifulSoup
import urllib.parse
import random
import httpx

app = FastAPI()

# Request Models
class TimeRequest(BaseModel):
    hour: Optional[int] = None

class ScrapeRequest(BaseModel):  # Unused but included for compatibility
    url: str

class ItemRequest(BaseModel):
    item_name: str

class TextRequest(BaseModel):
    text: str

class SearchRequest(BaseModel):
    query: str
    num_results: Optional[int] = 5

# Emoji mapping
EMOJI_MAP = {
    "morning": "☀️",
    "afternoon": "🌤️",
    "evening": "🌙",
    "night": "🌜",
    "hello": "👋",
    "hi": "😊",
    "hey": "🙌",
}

# Randomized user agents for scraping
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
]

HEADERS = {
    "User-Agent": random.choice(USER_AGENTS),
    "Accept-Language": "en-US,en;q=0.9",
}

# ScraperAPI key
API_KEY = "d6456f28759b1a12fb7fac335b5dc9f1"

# Greeting endpoint
@app.post("/greet", summary="Get a greeting message")
async def get_greeting(request: TimeRequest):
    now_utc = datetime.utcnow()
    now_bengaluru = now_utc + timedelta(hours=5, minutes=30)
    hour = request.hour if request.hour is not None else now_bengaluru.hour

    if not (0 <= hour <= 23):
        raise HTTPException(status_code=400, detail="Hour must be between 0 and 23.")

    if 5 <= hour < 12:
        greeting = "Good morning!"
    elif 12 <= hour < 18:
        greeting = "Good afternoon!"
    elif 18 <= hour < 22:
        greeting = "Good evening!"
    else:
        greeting = "Good night!"
    return {"greeting": greeting}

# Emoji-processed text
@app.post("/process", summary="Process text")
async def process_text(request: TextRequest):
    text = request.text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="Text cannot be empty.")
    emoji = next((emoji for keyword, emoji in EMOJI_MAP.items() if keyword in text.lower()), "✨")
    processed_text = f"{text} {emoji}"
    return {"result": processed_text}

# Web search
@app.post("/search", summary="Perform a web search")
async def web_search(request: SearchRequest):
    query = request.query.strip()
    num_results = request.num_results

    if not query:
        raise HTTPException(status_code=400, detail="Search query cannot be empty.")
    if not (1 <= num_results <= 20):
        raise HTTPException(status_code=400, detail="Number of results must be between 1 and 20.")

    try:
        results = []
        for i, url in enumerate(search(query, num_results=num_results, lang="en")):
            results.append({"id": i + 1, "url": url})
        return {"results": results} if results else {"results": [], "message": "No results found."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

# Async HTML fetch using ScraperAPI
async def fetch_html(url):
    proxy_url = f"http://api.scraperapi.com?api_key={API_KEY}&url={urllib.parse.quote_plus(url)}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(proxy_url)
            response.raise_for_status()
            return response.text
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch HTML: {str(e)}")

# Amazon scraper
@app.post("/scrape_amazon", summary="Scrape Amazon search results by item name")
async def scrape_amazon(request: ItemRequest):
    item_name = request.item_name.strip()
    if not item_name:
        raise HTTPException(status_code=400, detail="Item name cannot be empty.")

    search_query = urllib.parse.quote(item_name)
    search_url = f"https://www.amazon.com/s?k={search_query}"
    html = await fetch_html(search_url)

    soup = BeautifulSoup(html, "html.parser")
    products = soup.find_all("div", {"data-component-type": "s-search-result"})
    if not products:
        return {"results": [], "message": "No products found on Amazon."}

    results = []
    for product in products:
        title = product.find("h2", class_="a-size-mini")
        product_name = title.find("span").get_text(strip=True) if title and title.find("span") else "Not found"

        price = product.find("span", class_="a-offscreen")
        product_price = price.get_text(strip=True) if price else "Not found"

        link = product.find("a", class_="a-link-normal")
        product_url = "https://www.amazon.com" + link["href"] if link and "href" in link.attrs else search_url

        results.append({
            "product_name": product_name,
            "price": product_price,
            "url": product_url
        })

    return {"results": results}

# Walmart scraper using ScraperAPI
@app.post("/scrape_walmart", summary="Scrape Walmart search results by item name")
async def scrape_walmart(request: ItemRequest):
    item_name = request.item_name.strip()
    if not item_name:
        raise HTTPException(status_code=400, detail="Item name cannot be empty.")

    search_query = urllib.parse.quote(item_name)
    search_url = f"https://www.walmart.com/search?q={search_query}"
    html = await fetch_html(search_url)

    soup = BeautifulSoup(html, "html.parser")
    products = soup.find_all("div", {"data-item-id": True})
    if not products:
        return {"results": [], "message": "No products found on Walmart."}

    results = []
    for product in products:
        # Title
        title = product.find("span", {"data-automation-id": "product-title"})
        product_name = title.get_text(strip=True) if title else "Not found"

        # Price
        product_price = "Not found"
        price_div = product.find("div", {"data-automation-id": "product-price"})
        if price_div:
            # Look for spans containing $ and digits
            dollar_span = price_div.find("span", string=lambda s: s and "$" in s)
            major_span = price_div.find("span", string=lambda s: s and s.strip().isdigit())
            minor_span = major_span.find_next_sibling("span") if major_span else None

            if dollar_span and major_span:
                cents = minor_span.get_text(strip=True) if minor_span else "00"
                product_price = f"{dollar_span.get_text(strip=True)}{major_span.get_text(strip=True)}.{cents}"

        # URL
        link = product.find("a", {"link-identifier": True})
        if link and "href" in link.attrs:
            href = link["href"]
            if "rd=" in href:
                parsed = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
                product_url = parsed.get("rd", [href])[0]
            else:
                product_url = "https://www.walmart.com" + href
        else:
            product_url = search_url

        results.append({
            "product_name": product_name,
            "price": product_price,
            "url": product_url
        })

    return {"results": results}


# Target scraper using ScraperAPI
@app.post("/scrape_target", summary="Scrape Target search results by item name")
async def scrape_target(request: ItemRequest):
    item_name = request.item_name.strip()
    if not item_name:
        raise HTTPException(status_code=400, detail="Item name cannot be empty.")

    search_query = urllib.parse.quote(item_name)
    search_url = f"https://www.target.com/s?searchTerm={search_query}"
    html = await fetch_html(search_url)

    soup = BeautifulSoup(html, "html.parser")
    product_cards = soup.select('[data-test="product-title"]')
    prices = soup.select('[data-test="current-price"]')

    if not product_cards:
        return {"results": [], "message": "No products found on Target."}

    results = []
    for i, card in enumerate(product_cards[:10]):  # Limit to top 10 for speed
        title = card.get_text(strip=True)
        link = card["href"] if card.has_attr("href") else ""
        full_url = "https://www.target.com" + link if link else search_url
        price = prices[i].get_text(strip=True) if i < len(prices) else "Not found"

        results.append({
            "product_name": title,
            "price": price,
            "url": full_url
        })

    return {"results": results}

# For local dev/testing
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)