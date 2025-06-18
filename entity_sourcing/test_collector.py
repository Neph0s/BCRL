#!/usr/bin/env python3
"""
Test script to verify QRank data parsing
"""

from entity_collector import WikidataEntityCollector

def test_qrank_parsing():
    collector = WikidataEntityCollector()
    
    # Test with just a few categories
    categories = [
        ("Fictional Characters", "Q95074"),  # Fictional character
        ("Books", "Q571"),                   # Book
    ]
    
    print("Testing QRank data parsing...")
    df = collector.collect_entities(categories, limit_per_category=50)
    
    print(f"\nSample entities with scores:")
    sample = df.head(10)
    for _, entity in sample.iterrows():
        print(f"- {entity['label']} ({entity['id']}) - Score: {entity['popularity_score']}")
    
    print(f"\nTop 5 most popular entities:")
    top_popular = df.nlargest(5, 'popularity_score')
    for _, entity in top_popular.iterrows():
        print(f"- {entity['label']} ({entity['id']}) - Score: {entity['popularity_score']}")

if __name__ == "__main__":
    test_qrank_parsing()