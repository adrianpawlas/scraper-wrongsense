import asyncio
import json
import re
import urllib.parse
from datetime import datetime
from typing import Optional
from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PlaywrightTimeout

CATEGORIES = [
    "bottoms",
    "tops",
    "minimal-caps",
    "accessories"
]

BASE_URL = "https://wrongsense.com"
PRODUCT_SELECTORS = [
    '.product-card',
    '[class*="product-grid"] a[href*="/products/"]',
    '.grid-view-item',
    'a[href*="/products/"][class*="product"]'
]


class WrongSenseScraper:
    def __init__(self):
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.playwright = None
        self.scraped_products = []

    async def init_browser(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled']
        )
        context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        self.page = await context.new_page()
        await self.page.route("*", lambda route: route.continue_())

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def wait_forproducts(self, timeout: int = 10000) -> bool:
        for selector in PRODUCT_SELECTORS:
            try:
                await self.page.wait_for_selector(selector, timeout=timeout // 2)
                return True
            except PlaywrightTimeout:
                continue
        try:
            await self.page.wait_for_selector('[class*="product"]', timeout=timeout // 2)
            return True
        except PlaywrightTimeout:
            return False

    async def scroll_to_load_all(self, max_scrolls: int = 50, scroll_pause: int = 2000):
        last_count = 0
        no_new_count = 0
        
        for scroll_num in range(max_scrolls):
            await self.page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await asyncio.sleep(scroll_pause / 1000)
            
            try:
                products = await self.page.query_selector_all('[class*="product"] a[href*="/products/"], a[href*="/products/"][class*="product"]')
                if not products:
                    products = await self.page.query_selector_all('a[href*="/products/"]')
                
                current_count = len(products)
                
                if current_count == last_count:
                    no_new_count += 1
                    if no_new_count >= 3:
                        break
                else:
                    no_new_count = 0
                    
                last_count = current_count
                print(f"Scroll {scroll_num + 1}: {current_count} products loaded")
                
            except Exception as e:
                print(f"Scroll error: {e}")
                no_new_count += 1
                if no_new_count >= 3:
                    break
        
        return last_count

    async def extract_product_links(self) -> list[str]:
        links = set()
        
        selectors = [
            'a[href*="/products/"]',
            '[class*="product-card"] a',
            '[class*="grid-view-item"] a',
            '.product-grid a[href*="/products/"]'
        ]
        
        for selector in selectors:
            try:
                elements = await self.page.query_selector_all(selector)
                for el in elements:
                    href = await el.get_attribute('href')
                    if href and '/products/' in href:
                        if href.startswith('/'):
                            href = BASE_URL + href
                        elif not href.startswith('http'):
                            href = BASE_URL + '/' + href
                        links.add(href.split('?')[0])
            except Exception:
                continue
        
        product_links = list(links)
        print(f"Found {len(product_links)} product links")
        return product_links

    async def scrape_category(self, category: str) -> list[dict]:
        url = f"{BASE_URL}/collections/{category}"
        print(f"\n{'='*60}")
        print(f"Scraping category: {category}")
        print(f"URL: {url}")
        print(f"{'='*60}\n")
        
        await self.page.goto(url, wait_until='domcontentloaded')
        await asyncio.sleep(3)
        
        await self.wait_forproducts()
        
        product_count = await self.scroll_to_load_all()
        
        product_links = await self.extract_product_links()
        
        return product_links

    async def scrape_product_page(self, product_url: str) -> dict:
        await self.page.goto(product_url, wait_until='domcontentloaded')
        await asyncio.sleep(2)
        
        product_data = {
            'product_url': product_url,
            'title': None,
            'brand': 'Wrong Sense',
            'price': None,
            'sale': None,
            'images': [],
            'metadata': {},
            'category': None,
            'gender': None,
            'sizes': [],
            'colors': []
        }
        
        try:
            title_el = await self.page.query_selector('h1, [class*="product-title"], [class*="title"]')
            if title_el:
                product_data['title'] = await title_el.inner_text()
        except Exception:
            pass
        
        try:
            price_selectors = [
                '[class*="product-price"]',
                '.price',
                '#price',
                '[class*="price-container"]',
                '.price-item--regular',
                '.price__sale'
            ]
            for selector in price_selectors:
                price_el = await self.page.query_selector(selector)
                if price_el:
                    price_text = await price_el.inner_text()
                    if price_text:
                        product_data['price'] = price_text.strip()
                        break
        except Exception:
            pass
        
        if not product_data['price']:
            try:
                meta_price = await self.page.evaluate('''() => {
                    const meta = document.querySelector('meta[property="product:price:amount"]');
                    if (meta) return meta.content;
                    const priceEl = document.querySelector('[data-price]');
                    if (priceEl) return priceEl.getAttribute('data-price');
                    return null;
                }''')
                if meta_price:
                    product_data['price'] = meta_price
            except Exception:
                pass
        
        if not product_data['price']:
            try:
                price_el = await self.page.query_selector('[class*="sale"]')
                if price_el:
                    product_data['sale'] = await price_el.inner_text()
            except Exception:
                pass
            
            try:
                price_el = await self.page.query_selector('[class*="original"]')
                if price_el:
                    product_data['price'] = await price_el.inner_text()
            except Exception:
                pass
        
        try:
            desc_selectors = [
                '[class*="product-description"]',
                '.description',
                '[class*="description-content"]'
            ]
            for selector in desc_selectors:
                desc_el = await self.page.query_selector(selector)
                if desc_el:
                    product_data['description'] = await desc_el.inner_text()
                    break
        except Exception:
            pass
        
        try:
            image_elements = await self.page.query_selector_all('[class*="product-image"] img, [class*="gallery"] img, #product-image img')
            for img in image_elements:
                src = await img.get_attribute('src')
                if src and src not in product_data['images']:
                    if src.startswith('//'):
                        src = 'https:' + src
                    product_data['images'].append(src)
        except Exception:
            pass
        
        try:
            size_elements = await self.page.query_selector_all('[class*="size"] button, [class*="size"] option, [data-role="size"] button, input[name="Size"] + label')
            for size in size_elements:
                size_text = await size.inner_text()
                if size_text and size_text not in product_data['sizes']:
                    product_data['sizes'].append(size_text.strip())
        except Exception:
            pass
        
        try:
            color_elements = await self.page.query_selector_all('[class*="color"] button, [class*="color"] option')
            for color in color_elements:
                color_text = await color.inner_text()
                if color_text and color_text not in product_data['colors']:
                    product_data['colors'].append(color_text.strip())
        except Exception:
            pass
        
        try:
            script_data = await self.page.evaluate('''() => {
                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                for (const script of scripts) {
                    try {
                        return script.textContent;
                    } catch (e) {}
                }
                return null;
            }''')
            if script_data:
                try:
                    json_data = json.loads(script_data)
                    if isinstance(json_data, dict):
                        if not product_data['title']:
                            product_data['title'] = json_data.get('name')
                        if not product_data['description']:
                            product_data['description'] = json_data.get('description')
                        if not product_data['price']:
                            offers = json_data.get('offer', json_data.get('offers', []))
                            if offers:
                                if isinstance(offers, list):
                                    offers = offers[0]
                                price_val = offers.get('price') or offers.get('highPrice')
                                currency = offers.get('priceCurrency') or 'EUR'
                                if price_val:
                                    product_data['price'] = f"{price_val} {currency}"
                        json_data['image'] = product_data.get('image')
                        product_data['metadata']['json_ld'] = json_data
                except json.JSONDecodeError:
                    pass
        except Exception:
            pass
        
        product_data['metadata']['scraped_at'] = datetime.now().isoformat()
        
        return product_data

    async def scrape_all_categories(self) -> list[str]:
        all_product_urls = []
        
        for category in CATEGORIES:
            try:
                product_links = await self.scrape_category(category)
                all_product_urls.extend(product_links)
                print(f"Category '{category}': {len(product_links)} products")
            except Exception as e:
                print(f"Error scraping category '{category}': {e}")
        
        all_product_urls = list(set(all_product_urls))
        print(f"\nTotal unique products: {len(all_product_urls)}")
        
        return all_product_urls

    async def scrape_all_products(self, product_urls: list[str], batch_size: int = 10) -> list[dict]:
        all_products = []
        
        for i, url in enumerate(product_urls):
            print(f"\n[{i+1}/{len(product_urls)}] Scraping: {url}")
            
            try:
                product_data = await self.scrape_product_page(url)
                product_data['source'] = 'scraper-wrongsense'
                product_data['category_url'] = self._extract_category_from_url(url)
                all_products.append(product_data)
                print(f"  Title: {product_data.get('title', 'N/A')}")
                print(f"  Images: {len(product_data.get('images', []))}")
            except Exception as e:
                print(f"  Error: {e}")
            
            if (i + 1) % batch_size == 0:
                print(f"\n--- Scraped {i+1}/{len(product_urls)} products ---")
        
        self.scraped_products = all_products
        return all_products

    def _extract_category_from_url(self, url: str) -> Optional[str]:
        for category in CATEGORIES:
            if category in url.lower():
                return category
        return None


async def scrape_wrongsense():
    scraper = WrongSenseScraper()
    
    try:
        await scraper.init_browser()
        
        product_urls = await scraper.scrape_all_categories()
        
        with open('product_urls.txt', 'w') as f:
            for url in product_urls:
                f.write(url + '\n')
        
        products_data = await scraper.scrape_all_products(product_urls)
        
        with open('scraped_products.json', 'w', encoding='utf-8') as f:
            json.dump(products_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n{'='*60}")
        print(f"Scraping complete!")
        print(f"Total products: {len(products_data)}")
        print(f"Product URLs saved to: product_urls.txt")
        print(f"Products data saved to: scraped_products.json")
        print(f"{'='*60}")
        
        return products_data
        
    finally:
        await scraper.close()


if __name__ == '__main__':
    asyncio.run(scrape_wrongsense())