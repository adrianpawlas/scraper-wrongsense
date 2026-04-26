import os
import io
import json
import time
import base64
import numpy as np
import torch
from PIL import Image
from typing import Optional
from transformers import AutoModel, AutoProcessor
import requests
from tqdm import tqdm


MODEL_NAME = "google/siglip-base-patch16-384"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
EMBEDDING_DELAY = 0.5


class SigLIPEmbedder:
    def __init__(self, model_name: str = MODEL_NAME, device: str = DEVICE):
        self.model_name = model_name
        self.device = device
        self.model: Optional[AutoModel] = None
        self.processor: Optional[AutoProcessor] = None
        
    def load_model(self):
        print(f"Loading SigLIP model: {self.model_name}")
        print(f"Device: {self.device}")
        
        self.processor = AutoProcessor.from_pretrained(self.model_name)
        self.model = AutoModel.from_pretrained(self.model_name)
        self.model.to(self.device)
        self.model.eval()
        
        print(f"Model loaded successfully!")

    def get_image_embedding(self, image_url: str) -> Optional[np.ndarray]:
        if not self.model:
            self.load_model()
            
        try:
            response = requests.get(image_url, timeout=30)
            response.raise_for_status()
            
            image = Image.open(io.BytesIO(response.content)).convert('RGB')
            
            inputs = self.processor(images=image, return_tensors="pt")
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            with torch.no_grad():
                vision_outputs = self.model.vision_model(**inputs)
                pooled = vision_outputs.pooler_output
                if pooled is not None:
                    embedding = pooled.detach().squeeze().cpu().numpy()
                else:
                    embedding = vision_outputs.last_hidden_state.mean(dim=1).detach().squeeze().cpu().numpy()
            
            return embedding
            
        except Exception as e:
            print(f"Error getting image embedding from {image_url}: {e}")
            return None

    def get_text_embedding(self, text: str) -> Optional[np.ndarray]:
        if not self.model:
            self.load_model()
            
        try:
            max_tokens = 64
            text = text[:500]
            inputs = self.processor(text=text, return_tensors="pt")
            if inputs['input_ids'].shape[1] > max_tokens:
                inputs['input_ids'] = inputs['input_ids'][:, :max_tokens]
                if 'attention_mask' in inputs:
                    inputs['attention_mask'] = inputs['attention_mask'][:, :max_tokens]
            
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            with torch.no_grad():
                text_outputs = self.model.text_model(**inputs)
                pooled = text_outputs.pooler_output
                if pooled is not None:
                    embedding = pooled.detach().squeeze().cpu().numpy()
                else:
                    embedding = text_outputs.last_hidden_state.mean(dim=1).detach().squeeze().cpu().numpy()
            
            return embedding
            
        except Exception as e:
            print(f"Error getting text embedding: {e}")
            return None

    def process_products_json(self, input_file: str = "scraped_products.json"):
        if not self.model:
            self.load_model()
            
        with open(input_file, 'r', encoding='utf-8') as f:
            products = json.load(f)
        
        processed_products = []
        
        print(f"\nProcessing {len(products)} products for embeddings...")
        
        for i, product in enumerate(tqdm(products)):
            processed = product.copy()
            
            images = product.get('images', [])
            
            if images:
                primary_image = images[0]
                img_embedding = self.get_image_embedding(primary_image)
                time.sleep(EMBEDDING_DELAY)
                processed['image_embedding'] = img_embedding.tolist() if img_embedding is not None else None
                processed['image_url'] = primary_image
                processed['additional_images'] = ", ".join(images[1:]) if len(images) > 1 else None
            else:
                processed['image_embedding'] = None
                processed['image_url'] = None
                processed['additional_images'] = None
            
            text_parts = []
            if product.get('title'):
                text_parts.append(product['title'])
            if product.get('description'):
                text_parts.append(product['description'])
            if product.get('price'):
                text_parts.append(f"Price: {product['price']}")
            if product.get('category'):
                text_parts.append(f"Category: {product['category']}")
            if product.get('gender'):
                text_parts.append(f"Gender: {product['gender']}")
            if product.get('sizes'):
                text_parts.append(f"Sizes: {', '.join(product['sizes'])}")
            if product.get('colors'):
                text_parts.append(f"Colors: {', '.join(product['colors'])}")
            if product.get('metadata'):
                text_parts.append(json.dumps(product['metadata']))
            
            full_text = " | ".join(text_parts)
            
            if full_text:
                text_embedding = self.get_text_embedding(full_text)
                time.sleep(EMBEDDING_DELAY)
                processed['info_embedding'] = text_embedding.tolist() if text_embedding is not None else None
            else:
                processed['info_embedding'] = None
            
            processed_products.append(processed)
            
            if (i + 1) % 10 == 0:
                print(f"\nProcessed {i+1}/{len(products)} products")
        
        with open('products_with_embeddings.json', 'w', encoding='utf-8') as f:
            json.dump(processed_products, f, ensure_ascii=False, indent=2)
        
        print(f"\nEmbedding processing complete!")
        print(f"Output saved to: products_with_embeddings.json")
        
        return processed_products


def process_products(input_file: str = "scraped_products.json"):
    embedder = SigLIPEmbedder()
    embedder.load_model()
    embedder.process_products_json(input_file)


if __name__ == '__main__':
    process_products()