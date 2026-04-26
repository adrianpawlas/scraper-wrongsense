import os
import json
import re
import uuid
import asyncio
from datetime import datetime
from typing import Optional
from supabase import create_client, Client
import numpy as np


SUPABASE_URL = "https://yqawmzggcgpeyaaynrjk.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxYXdtemdnY2dwZXlhYXlucmprIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NTAxMDkyNiwiZXhwIjoyMDcwNTg2OTI2fQ.XtLpxausFriraFJeX27ZzsdQsFv3uQKXBBggoz6P4D4"


def generate_product_id() -> str:
    return f"ws_{uuid.uuid4().hex[:12]}"


def parse_price(price_str: str) -> tuple[Optional[float], Optional[str]]:
    if not price_str:
        return None, None
    
    price_str = price_str.strip()
    price_str = re.sub(r'\s+', '', price_str)
    
    currency_map = {
        '$': 'USD', '€': 'EUR', '£': 'GBP', '¥': 'JPY',
        'USD': 'USD', 'EUR': 'EUR', 'GBP': 'GBP', 'JPY': 'JPY',
        'CZK': 'CZK', 'PLN': 'PLN', 'HUF': 'HUF', 'RON': 'RON'
    }
    
    currency = 'EUR'
    for sym, curr in currency_map.items():
        if sym in price_str.upper():
            currency = curr
            price_str = price_str.replace(sym, '').replace(sym.lower(), '')
            break
    
    try:
        price_value = float(price_str.replace(',', '.'))
    except ValueError:
        return None, None
    
    return price_value, currency


def format_price(value: float, currency: str) -> str:
    if currency == 'EUR':
        return f"{value:.2f}EUR"
    return f"{value}{currency}"


def parse_prices(price_data: str) -> list[str]:
    if not price_data:
        return []
    
    prices = []
    
    cleaned = re.sub(r'\s+', '', price_data)
    
    currency_map = {
        'EUR': 'EUR', 'USD': 'USD', 'GBP': 'GBP', 'JPY': 'JPY',
        'CZK': 'CZK', 'PLN': 'PLN', 'HUF': 'HUF', 'RON': 'RON',
        '€': 'EUR', '$': 'USD', '£': 'GBP', '¥': 'JPY'
    }
    
    for curr, std in currency_map.items():
        pattern = rf'([{re.escape(curr)}]?[\d,]+\.?\d*{re.escape(curr)}?)'
        matches = re.findall(pattern, cleaned, re.IGNORECASE)
        for match in matches:
            m = match.strip()
            for sym, std_curr in currency_map.items():
                if sym in m.upper():
                    m = m.replace(sym, '').replace(sym.lower(), '')
                    try:
                        val = float(m.replace(',', '.'))
                        prices.append(format_price(val, std_curr))
                        break
                    except ValueError:
                        pass
    
    if not prices and price_data:
        value, currency = parse_price(price_data)
        if value is not None:
            prices.append(format_price(value, currency))
    
    return prices


def extract_category_from_url(url: str) -> list[str]:
    url_lower = url.lower()
    
    category_map = {
        'bottoms': ['bottoms', 'pants', 'shorts', 'jeans', 'trousers', 'skirt', 'leggings'],
        'tops': ['tops', 'shirt', 't-shirt', 'hoodie', 'sweater', 'jacket', 'coat', 'vest', 'polo'],
        'minimal-caps': ['caps', 'cap', 'hat', 'beanie', 'headwear'],
        'accessories': ['accessories', 'bag', 'belt', 'scarf', 'sock', 'wallet', 'keychain']
    }
    
    categories = []
    
    for cat, keywords in category_map.items():
        for kw in keywords:
            if kw in url_lower:
                if cat == 'bottoms':
                    categories.append('Bottoms')
                elif cat == 'tops':
                    if 'jacket' in url_lower or 'coat' in url_lower:
                        categories.append('Outerwear')
                    elif 'hoodie' in url_lower:
                        categories.append('Hoodies')
                    elif 'sweater' in url_lower:
                        categories.append('Sweaters')
                    else:
                        categories.append('Tops')
                elif cat == 'minimal-caps':
                    categories.append('Caps')
                elif cat == 'accessories':
                    categories.append('Accessories')
                break
    
    if not categories:
        categories.append('General')
    
    return categories


def determine_gender(title: str, description: str = '') -> Optional[str]:
    text = f"{title} {description}".lower()
    
    if any(w in text for w in ['women', 'woman', 'ladies', 'lady', 'femmes']):
        return 'Women'
    elif any(w in text for w in ['men', 'man', 'homme', 'guys']):
        return 'Men'
    elif any(w in text for w in ['unisex', 'unissex']):
        return 'Unisex'
    
    return None


def format_size(sizes: list) -> str:
    if not sizes:
        return None
    return ", ".join([str(s).strip() for s in sizes if s])


def format_additional_images(images: list) -> Optional[str]:
    if not images or len(images) <= 1:
        return None
    return ", ".join([img.strip() for img in images if img])


def convert_embedding_to_sql(embedding) -> str:
    if embedding is None:
        return None
    
    if isinstance(embedding, list):
        embedding = np.array(embedding)
    
    if isinstance(embedding, np.ndarray):
        return '[' + ','.join(map(str, embedding.tolist())) + ']'
    
    return None


def format_metadata(metadata: dict) -> str:
    if not metadata:
        return None
    return json.dumps(metadata)


class SupabaseImporter:
    def __init__(self, url: str = SUPABASE_URL, key: str = SUPABASE_ANON_KEY):
        self.url = url
        self.key = key
        self.client: Optional[Client] = None
        
    def connect(self):
        self.client = create_client(self.url, self.key)
        print("Connected to Supabase!")
        
    def import_products(self, products_file: str = "products_with_embeddings.json", batch_size: int = 10):
        if not self.client:
            self.connect()
            
        with open(products_file, 'r', encoding='utf-8') as f:
            products = json.load(f)
        
        imported_count = 0
        error_count = 0
        
        print(f"\nImporting {len(products)} products to Supabase...")
        
        for i, product in enumerate(products):
            try:
                product_id = generate_product_id()
                
                categories = extract_category_from_url(product.get('product_url', ''))
                category_str = ", ".join(categories)
                
                title = product.get('title', '')
                desc = product.get('description', '')
                gender = determine_gender(title, desc)
                
                price_raw = product.get('price', '')
                sale_raw = product.get('sale', '')
                
                parsed_prices = parse_prices(price_raw)
                price_str = ", ".join(parsed_prices) if parsed_prices else None
                
                sale_price = None
                if sale_raw:
                    sale_prices = parse_prices(sale_raw)
                    sale_price = ", ".join(sale_prices) if sale_prices else None
                
                sizes = format_size(product.get('sizes', []))
                
                additional_images = format_additional_images(product.get('images', []))
                
                image_url = product.get('image_url')
                
                metadata = {
                    'title': title,
                    'description': desc,
                    'sizes': product.get('sizes', []),
                    'colors': product.get('colors', []),
                    'price_raw': price_raw,
                    'sale_raw': sale_raw,
                    'original_category': product.get('category'),
                }
                metadata_str = format_metadata(metadata)
                
                image_embedding_list = product.get('image_embedding')
                info_embedding_list = product.get('info_embedding')
                
                image_embedding_sql = convert_embedding_to_sql(image_embedding_list)
                info_embedding_sql = convert_embedding_to_sql(info_embedding_list)
                
                record = {
                    'id': product_id,
                    'source': 'scraper-wrongsense',
                    'product_url': product.get('product_url'),
                    'brand': 'Wrong Sense',
                    'image_url': image_url,
                    'title': title,
                    'description': desc,
                    'category': category_str,
                    'gender': gender,
                    'price': price_str,
                    'sale': sale_price,
                    'second_hand': False,
                    'metadata': metadata_str,
                    'size': sizes,
                    'additional_images': additional_images,
                    'image_embedding': image_embedding_sql,
                    'info_embedding': info_embedding_sql,
                    'created_at': datetime.now().isoformat()
                }
                
                self.client.table('products').insert(record).execute()
                
                imported_count += 1
                
                if (i + 1) % batch_size == 0:
                    print(f"Imported {i+1}/{len(products)} products...")
                    
            except Exception as e:
                error_count += 1
                print(f"Error importing product {i+1}: {e}")
                if error_count <= 5:
                    continue
                else:
                    break
        
        print(f"\n{'='*60}")
        print(f"Import complete!")
        print(f"Successfully imported: {imported_count}")
        print(f"Errors: {error_count}")
        print(f"{'='*60}")
        
        return imported_count

    def update_product(self, product_id: str, updates: dict):
        if not self.client:
            self.connect()
            
        try:
            self.client.table('products').update(updates).eq('id', product_id).execute()
            print(f"Product {product_id} updated!")
        except Exception as e:
            print(f"Error updating product: {e}")

    def get_all_products(self, limit: int = 100):
        if not self.client:
            self.connect()
            
        try:
            response = self.client.table('products').select('*').limit(limit).execute()
            return response.data
        except Exception as e:
            print(f"Error getting products: {e}")
            return []

    def delete_all_products(self):
        if not self.client:
            self.connect()
            
        try:
            self.client.table('products').delete().neq('id', '').execute()
            print("All products deleted!")
        except Exception as e:
            print(f"Error deleting products: {e}")


def import_to_supabase(products_file: str = "products_with_embeddings.json"):
    importer = SupabaseImporter()
    importer.connect()
    importer.import_products(products_file)


if __name__ == '__main__':
    import_to_supabase()