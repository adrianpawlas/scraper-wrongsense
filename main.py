#!/usr/bin/env python3
"""
Wrong Sense Scraper - Main Orchestration Script
================================================

This script orchestrates the complete scraping pipeline:
1. Scrape all products from Wrong Sense website
2. Generate embeddings (image and text) using SigLIP
3. Import all data to Supabase

Usage:
    python main.py              # Run full pipeline
    python main.py --scrape     # Only scrape products
    python main.py --embed      # Only generate embeddings
    python main.py --import    # Only import to Supabase
    python main.py --all        # Full pipeline (default)
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

from scraper import scrape_wrongsense
from embedder import SigLIPEmbedder, process_products as embed_products
from database import SupabaseImporter, import_to_supabase


def parse_args():
    parser = argparse.ArgumentParser(
        description='Wrong Sense Scraper - Full Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--scrape', 
        action='store_true',
        help='Only scrape products from website'
    )
    parser.add_argument(
        '--embed', 
        action='store_true',
        help='Only generate embeddings'
    )
    parser.add_argument(
        '--import-db', 
        action='store_true',
        help='Only import to Supabase'
    )
    parser.add_argument(
        '--all', 
        action='store_true',
        default=True,
        help='Run full pipeline (default)'
    )
    parser.add_argument(
        '--input-scraped',
        type=str,
        default='scraped_products.json',
        help='Input file for scraped products (default: scraped_products.json)'
    )
    parser.add_argument(
        '--input-embedded',
        type=str,
        default='products_with_embeddings.json',
        help='Input file for products with embeddings (default: products_with_embeddings.json)'
    )
    
    return parser.parse_args()


def run_scrape():
    print("\n" + "="*60)
    print("STEP 1: SCRAPING PRODUCTS FROM WRONG SENSE")
    print("="*60 + "\n")
    
    products = asyncio.run(scrape_wrongsense())
    
    print(f"\n{'='*60}")
    print(f"Scraping step complete!")
    print(f"Total products scraped: {len(products)}")
    print(f"{'='*60}\n")
    
    return len(products) > 0


def run_embed(input_file: str):
    print("\n" + "="*60)
    print("STEP 2: GENERATING EMBEDDINGS (SigLIP)")
    print("="*60 + "\n")
    
    if not Path(input_file).exists():
        print(f"Error: Input file '{input_file}' not found!")
        print("Run scraping first: python main.py --scrape")
        return False
    
    try:
        embed_products(input_file)
        print(f"\n{'='*60}")
        print(f"Embedding step complete!")
        print(f"{'='*60}\n")
        return True
    except Exception as e:
        print(f"Error generating embeddings: {e}")
        return False


def run_import_db(input_file: str):
    print("\n" + "="*60)
    print("STEP 3: IMPORTING TO SUPABASE")
    print("="*60 + "\n")
    
    if not Path(input_file).exists():
        print(f"Error: Input file '{input_file}' not found!")
        print("Run embedding first: python main.py --embed")
        return False
    
    try:
        import_to_supabase(input_file)
        print(f"\n{'='*60}")
        print(f"Import step complete!")
        print(f"{'='*60}\n")
        return True
    except Exception as e:
        print(f"Error importing to Supabase: {e}")
        return False


def main():
    args = parse_args()
    
    print("\n" + "#"*60)
    print("# WRONG SENSE SCRAPER - FULL PIPELINE")
    print("#"*60 + "\n")
    
    scrape_success = True
    embed_success = True
    import_success = True
    
    if args.scrape:
        scrape_success = run_scrape()
    elif args.embed:
        embed_success = run_embed(args.input_scraped)
    elif args.import_db:
        import_success = run_import_db(args.input_embedded)
    else:
        scrape_success = run_scrape()
        
        if scrape_success:
            embed_success = run_embed(args.input_scraped)
        
        if embed_success:
            import_success = run_import_db(args.input_embedded)
    
    print("\n" + "#"*60)
    print("# PIPELINE SUMMARY")
    print("#"*60)
    print(f"# Scraping:     {'✓ SUCCESS' if scrape_success else '✗ FAILED'}")
    print(f"# Embeddings:  {'✓ SUCCESS' if embed_success else '✗ FAILED'}")
    print(f"# Import:      {'✓ SUCCESS' if import_success else '✗ FAILED'}")
    print("#"*60 + "\n")
    
    if scrape_success and embed_success and import_success:
        print("✓ Full pipeline completed successfully!")
        return 0
    else:
        print("✗ Some steps failed. Check logs above.")
        return 1


if __name__ == '__main__':
    sys.exit(main())