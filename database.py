import os
import json
import re
import time
import uuid
import logging
from datetime import datetime
from typing import Optional
from supabase import create_client, Client
import numpy as np


logging.basicConfig(filename='failed_products.log', level=logging.ERROR)
logger = logging.getLogger(__name__)

SUPABASE_URL = "https://yqawmzggcgpeyaaynrjk.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxYXdtemdnY2dwZXlhYXlucmprIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NTAxMDkyNiwiZXhwIjoyMDcwNTg2OTI2fQ.XtLpxausFriraFJeX27ZzsdQsFv3uQKXBBggoz6P4D4"

BATCH_SIZE = 50
MAX_RETRIES = 3
STALE_THRESHOLD_RUNS = 2
EMBEDDING_DELAY = 0.5


class SupabaseImporter:
    def __init__(self, url: str = SUPABASE_URL, key: str = SUPABASE_ANON_KEY):
        self.url = url
        self.key = key
        self.client: Optional[Client] = None
        self.stats = {
            'new': 0,
            'updated': 0,
            'unchanged': 0,
            'deleted': 0,
            'failed': 0
        }
        self.seen_product_urls: set = set()
        
    def connect(self):
        self.client = create_client(self.url, self.key)
        print("Connected to Supabase!")

    def format_embedding(self, embedding) -> str:
        if embedding is None:
            return None
        if isinstance(embedding, list):
            embedding = np.array(embedding)
        if isinstance(embedding, np.ndarray):
            return '[' + ','.join(map(str, embedding.tolist())) + ']'
        return None

    def parse_price(self, price_str: str) -> tuple[Optional[float], Optional[str]]:
        if not price_str:
            return None, None
        price_str = re.sub(r'\s+', '', price_str.strip())
        currency_map = {'$': 'USD', '€': 'EUR', '£': 'GBP', '¥': 'JPY', 
                       'CZK': 'CZK', 'PLN': 'PLN', 'HUF': 'HUF', 'RON': 'RON'}
        currency = 'EUR'
        for sym, curr in currency_map.items():
            if sym in price_str.upper():
                currency = curr
                price_str = price_str.replace(sym, '').replace(sym.lower(), '')
                break
        try:
            return float(price_str.replace(',', '.')), currency
        except ValueError:
            return None, None

    def format_price_output(self, value: float, currency: str) -> str:
        if currency == 'EUR':
            return f"{value:.2f}EUR"
        return f"{value}{currency}"

    def parse_prices(self, price_data: str) -> list[str]:
        if not price_data:
            return []
        clean = re.sub(r'\s+', '', price_data)
        prices = []
        currency_map = {'EUR': 'EUR', 'USD': 'USD', 'GBP': 'GBP', 'JPY': 'JPY',
                     'CZK': 'CZK', 'PLN': 'PLN', 'HUF': 'HUF', 'RON': 'RON',
                     '€': 'EUR', '$': 'USD', '£': 'GBP', '¥': 'JPY'}
        for curr, std in currency_map.items():
            pattern = rf'([{re.escape(curr)}]?[\d,]+\.?\d*{re.escape(curr)}?)'
            matches = re.findall(pattern, clean, re.IGNORECASE)
            for m in matches:
                m = m.strip()
                for sym, std_curr in currency_map.items():
                    if sym in m.upper():
                        m = m.replace(sym, '').replace(sym.lower(), '')
                        try:
                            val = float(m.replace(',', '.'))
                            prices.append(self.format_price_output(val, std_curr))
                            break
                        except ValueError:
                            pass
        if not prices and price_data:
            value, currency = self.parse_price(price_data)
            if value is not None:
                prices.append(self.format_price_output(value, currency))
        return prices

    def extract_category_from_url(self, url: str) -> str:
        url_lower = url.lower()
        if 'bottoms' in url_lower:
            return 'Bottoms'
        elif any(k in url_lower for k in ['jacket', 'coat']):
            return 'Outerwear'
        elif 'hoodie' in url_lower:
            return 'Hoodies'
        elif 'sweater' in url_lower:
            return 'Sweaters'
        elif 'tops' in url_lower:
            return 'Tops'
        elif any(k in url_lower for k in ['caps', 'cap', 'hat', 'beanie']):
            return 'Caps'
        elif 'accessories' in url_lower:
            return 'Accessories'
        return 'General'

    def determine_gender(self, title: str, description: str = '') -> Optional[str]:
        text = f"{title} {description}".lower()
        if any(w in text for w in ['women', 'woman', 'ladies', 'lady', 'femmes']):
            return 'Women'
        elif any(w in text for w in ['men', 'man', 'homme', 'guys']):
            return 'Men'
        elif any(w in text for w in ['unisex', 'unissex']):
            return 'Unisex'
        return None

    def normalize_for_comparison(self, data: dict) -> dict:
        normalized = {}
        for key in ['title', 'description', 'price', 'sale', 'image_url', 'additional_images', 'size', 'gender']:
            value = data.get(key)
            if value is None:
                normalized[key] = ''
            elif isinstance(value, str):
                normalized[key] = value.strip().lower()
            else:
                normalized[key] = value
        return normalized

    def has_changed(self, existing: dict, new_data: dict) -> bool:
        if not existing:
            return True
        new_norm = self.normalize_for_comparison(new_data)
        existing_norm = self.normalize_for_comparison(existing)
        for key in ['title', 'description', 'price', 'sale', 'image_url', 'additional_images', 'size', 'gender']:
            if new_norm.get(key, '') != existing_norm.get(key, ''):
                return True
        return False

    def needs_new_embedding(self, existing: dict, new_data: dict) -> bool:
        if not existing:
            return True
        existing_url = existing.get('image_url', '') or ''
        new_url = new_data.get('image_url') or ''
        return existing_url.strip().lower() != new_url.strip().lower()

    def get_existing_products(self) -> dict:
        try:
            response = self.client.table('products').select('id, product_url, image_url, title, updated_at').eq('source', 'scraper-wrongsense').execute()
            return {p['product_url']: p for p in response.data}
        except Exception as e:
            print(f"Error fetching existing products: {e}")
            return {}

    def build_record(self, product: dict, image_embedding=None, info_embedding=None) -> dict:
        product_id = f"ws_{uuid.uuid4().hex[:12]}"
        title = product.get('title', '')
        desc = product.get('description', '')
        product_url = product.get('product_url', '')
        gender = self.determine_gender(title, desc)
        category = self.extract_category_from_url(product_url)
        price_raw = product.get('price', '')
        sale_raw = product.get('sale', '')
        parsed_prices = self.parse_prices(price_raw)
        price_str = ", ".join(parsed_prices) if parsed_prices else None
        sale_price = None
        if sale_raw:
            sale_prices = self.parse_prices(sale_raw)
            sale_price = ", ".join(sale_prices) if sale_prices else None
        sizes = product.get('sizes', [])
        size_str = ", ".join([str(s).strip() for s in sizes]) if sizes else None
        images = product.get('images', [])
        image_url = images[0] if images else None
        additional = ", ".join([img.strip() for img in images[1:]]) if len(images) > 1 else None
        metadata = {
            'title': title,
            'description': desc,
            'sizes': sizes,
            'colors': product.get('colors', []),
            'price_raw': price_raw,
            'sale_raw': sale_raw,
        }
        return {
            'id': product_id,
            'source': 'scraper-wrongsense',
            'product_url': product_url,
            'brand': 'Wrong Sense',
            'image_url': image_url,
            'title': title,
            'description': desc,
            'category': category,
            'gender': gender,
            'price': price_str,
            'sale': sale_price,
            'second_hand': False,
            'metadata': json.dumps(metadata),
            'size': size_str,
            'additional_images': additional,
            'image_embedding': self.format_embedding(image_embedding),
            'info_embedding': self.format_embedding(info_embedding),
            'updated_at': datetime.now().isoformat()
        }

    def batch_upsert(self, batch: list, existing_products: dict, products_with_embeddings: list) -> None:
        records_to_insert = []
        existing_by_url = {p['product_url']: p for p in existing_products}
        
        for product in products_with_embeddings:
            product_url = product.get('product_url', '')
            if not product_url:
                continue
            self.seen_product_urls.add(product_url)
            image_embedding = product.get('image_embedding')
            info_embedding = product.get('info_embedding')
            existing = existing_by_url.get(product_url)
            if existing:
                if not self.has_changed(existing, product):
                    self.stats['unchanged'] += 1
                    continue
                if self.needs_new_embedding(existing, product):
                    pass
                else:
                    image_embedding = None
                    info_embedding = None
                record = self.build_record(product, image_embedding, info_embedding)
                record['id'] = existing['id']
                self.stats['updated'] += 1
            else:
                record = self.build_record(product, image_embedding, info_embedding)
                self.stats['new'] += 1
            records_to_insert.append(record)
        
        if not records_to_insert:
            return
        
        for attempt in range(MAX_RETRIES):
            try:
                self.client.table('products').upsert(records_to_insert, on_conflict='source,product_url').execute()
                return
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    print(f"Batch insert failed after {MAX_RETRIES} retries: {e}")
                    for rec in records_to_insert:
                        logger.error(f"Failed: {rec.get('product_url')}")
                    self.stats['failed'] += len(records_to_insert)
                time.sleep(1)

    def import_products(self, products_file: str = "products_with_embeddings.json"):
        if not self.client:
            self.connect()
        with open(products_file, 'r', encoding='utf-8') as f:
            products = json.load(f)
        existing_products = self.get_existing_products()
        for i in range(0, len(products), BATCH_SIZE):
            batch = products[i:i+BATCH_SIZE]
            self.batch_upsert(batch, existing_products, batch)
            print(f"Processed {min(i+BATCH_SIZE, len(products))}/{len(products)} products... (new: {self.stats['new']}, updated: {self.stats['updated']}, unchanged: {self.stats['unchanged']})")
        self.cleanup_stale_products(existing_products)
        self.print_summary()

    def cleanup_stale_products(self, existing_products: dict):
        stale_threshold = datetime.now().timestamp() - (STALE_THRESHOLD_RUNS * 3 * 24 * 3600)
        if not self.seen_product_urls:
            return
        stale = []
        for url, data in existing_products.items():
            if url not in self.seen_product_urls:
                try:
                    updated = data.get('updated_at')
                    if updated:
                        ts = datetime.fromisoformat(updated.replace('Z', '+00:00')).timestamp()
                        if ts < stale_threshold:
                            stale.append(data['id'])
                except:
                    stale.append(data['id'])
        if stale:
            for i in range(0, len(stale), BATCH_SIZE):
                batch = stale[i:i+BATCH_SIZE]
                for attempt in range(MAX_RETRIES):
                    try:
                        self.client.table('products').delete().in_('id', batch).execute()
                        self.stats['deleted'] += len(batch)
                        break
                    except Exception as e:
                        if attempt == MAX_RETRIES - 1:
                            print(f"Delete failed: {e}")
                        time.sleep(1)

    def print_summary(self):
        print(f"\n{'='*60}")
        print(f"IMPORT SUMMARY")
        print(f"{'='*60}")
        print(f"New products added:        {self.stats['new']}")
        print(f"Products updated:         {self.stats['updated']}")
        print(f"Products unchanged:     {self.stats['unchanged']}")
        print(f"Stale products deleted:  {self.stats['deleted']}")
        print(f"Failed products:        {self.stats['failed']}")
        print(f"{'='*60}\n")


def import_to_supabase(products_file: str = "products_with_embeddings.json"):
    importer = SupabaseImporter()
    importer.connect()
    importer.import_products(products_file)


if __name__ == '__main__':
    import_to_supabase()