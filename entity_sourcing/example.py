#!/usr/bin/env python3
"""
Simple example: Collect Wikidata entities with popularity ranking
Usage: python example.py
"""

from entity_collector import WikidataEntityCollector

def main():
    # Create collector
    collector = WikidataEntityCollector(
        language='zh',  # Chinese entities
        qrank_csv_file='qrank.csv',
        major_sample_size=100,  # Major categories: 100 entities each
        minor_sample_size=30    # Minor categories: 30 entities each
    )
    
    # Define categories to collect
    categories = [
        # Major categories (high research value)
        ("People - Humans", "Q5", "major"),                     # Real people
        ("People - Fictional Characters", "Q95074", "major"),
        ("Works - Books", "Q571", "major"), 
        ("Places - Cities", "Q515", "major"),
        ("Organizations - Companies", "Q783794", "major"),
        
        # Minor categories (specialized)
        ("Nature - Fish", "Q152", "minor"),
        ("Events - Festivals", "Q132241", "minor"),
        ("Culture - Cuisines", "Q1968435", "minor"),
    ]
    
    # Collect entities
    print("Starting entity collection...")
    df = collector.collect_entities(categories)
    
    # Save results (sorted by popularity, high to low)
    collector.save_results(df, "collected_entities.csv")
    
    # Show summary
    print(f"\n=== SUMMARY ===")
    print(f"Total entities: {len(df)}")
    if not df.empty:
        print(f"Top 5 most popular:")
        for i, (_, entity) in enumerate(df.head(5).iterrows(), 1):
            print(f"  {i}. {entity['label']} - {entity['popularity_score']:,.0f} ({entity['category']})")

if __name__ == "__main__":
    main()