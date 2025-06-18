#!/usr/bin/env python3
"""
Optimized QRank data loader with caching
"""

import csv
import pickle
import os
from typing import Dict

class QRankLoader:
    def __init__(self, csv_file='qrank.csv', cache_file='qrank_cache.pkl'):
        self.csv_file = csv_file
        self.cache_file = cache_file
    
    def load_qrank_data(self, use_cache=True) -> Dict[str, float]:
        """Load QRank data with caching"""
        
        # Try cache first
        if use_cache and os.path.exists(self.cache_file):
            try:
                print(f"Loading QRank data from cache: {self.cache_file}")
                with open(self.cache_file, 'rb') as f:
                    data = pickle.load(f)
                print(f"Loaded {len(data)} entities from cache")
                return data
            except Exception as e:
                print(f"Cache loading failed: {e}")
        
        # Load from CSV and cache
        return self._load_and_cache_csv()
    
    def _load_and_cache_csv(self) -> Dict[str, float]:
        """Load CSV data and create cache"""
        print(f"Loading QRank data from CSV: {self.csv_file}")
        print("This may take a few minutes for large files...")
        
        data = {}
        try:
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                
                for row_num, row in enumerate(reader):
                    if len(row) >= 2:
                        entity_id = row[0]
                        try:
                            score = float(row[1])
                            data[entity_id] = score
                        except ValueError:
                            continue
                    
                    # Progress every 500k rows
                    if row_num % 500000 == 0 and row_num > 0:
                        print(f"  Processed {row_num:,} rows...")
            
            print(f"Loaded {len(data):,} entities from CSV")
            
            # Save cache
            try:
                print("Saving cache...")
                with open(self.cache_file, 'wb') as f:
                    pickle.dump(data, f)
                print(f"Cache saved to: {self.cache_file}")
            except Exception as e:
                print(f"Warning: Could not save cache: {e}")
            
            return data
            
        except FileNotFoundError:
            print(f"Error: CSV file not found: {self.csv_file}")
            return {}
        except Exception as e:
            print(f"Error loading CSV: {e}")
            return {}
    
    def get_score(self, entity_id: str, data: Dict[str, float]) -> float:
        """Get popularity score for an entity"""
        return data.get(entity_id, 0.0)

def main():
    """Test the QRank loader"""
    loader = QRankLoader()
    
    # Load data
    data = loader.load_qrank_data()
    
    if data:
        print(f"\nQRank data loaded successfully: {len(data):,} entities")
        
        # Test some entities
        test_entities = ['Q178995', 'Q866', 'Q635', 'Q999999999']
        print("\nSample scores:")
        for entity_id in test_entities:
            score = loader.get_score(entity_id, data)
            print(f"  {entity_id}: {score:,.0f}")
    else:
        print("Failed to load QRank data")

if __name__ == "__main__":
    main()