#!/usr/bin/env python3
"""
Wikidata Entity Collector with Popularity Ranking

This script collects Wikidata entities (instances) and weights them by popularity
using QRank data for selecting unpopular entities in subsequent steps.
"""

import requests
import pandas as pd
from typing import Dict, List, Optional, Tuple
import time
from qrank_loader import QRankLoader


class WikidataEntityCollector:
    def __init__(self, language='en', qrank_csv_file='qrank.csv'):
        self.sparql_endpoint = "https://query.wikidata.org/sparql"
        self.qrank_data = {}
        self.language = language  # Support for different languages
        self.qrank_loader = QRankLoader(csv_file=qrank_csv_file)
        
    def load_qrank_data(self) -> Dict[str, float]:
        """Load QRank popularity data using optimized loader."""
        return self.qrank_loader.load_qrank_data()
    
    def query_wikidata_entities(self, instance_of: str = None, limit: int = 1000) -> List[Dict]:
        """
        Query Wikidata for entities that are instances of a given type.
        
        Args:
            instance_of: Wikidata ID (e.g., 'Q5' for human, 'Q95074' for fictional character).
                        If None, queries entities without type constraint.
            limit: Maximum number of entities to retrieve
        """
        
        # Build query based on whether instance_of is specified
        if instance_of:
            type_constraint = f"?item wdt:P31 wd:{instance_of} ."
            query_desc = f"instances of {instance_of}"
        else:
            # For mixed entities, use a more specific approach to avoid timeout
            # Sample from multiple common types rather than all possible types
            type_constraint = """
            {
              { ?item wdt:P31 wd:Q5 } UNION          # humans
              { ?item wdt:P31 wd:Q515 } UNION        # cities  
              { ?item wdt:P31 wd:Q571 } UNION        # books
              { ?item wdt:P31 wd:Q11424 } UNION      # films
              { ?item wdt:P31 wd:Q783794 } UNION     # companies
              { ?item wdt:P31 wd:Q95074 } UNION      # fictional characters
              { ?item wdt:P31 wd:Q7366 } UNION       # songs
              { ?item wdt:P31 wd:Q3918 }             # universities
            }"""
            query_desc = "mixed entity types (sampled)"
        
        query = f"""
        SELECT DISTINCT ?item ?itemLabel ?itemDescription WHERE {{
          {type_constraint}
          ?item rdfs:label ?itemLabel .
          FILTER(LANG(?itemLabel) = "{self.language}")
          OPTIONAL {{ ?item schema:description ?itemDescription . FILTER(LANG(?itemDescription) = "{self.language}") }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{self.language}" }}
        }}
        LIMIT {limit}
        """
        
        headers = {
            'Accept': 'application/sparql-results+json',
            'User-Agent': 'EntityCollector/1.0 (https://example.com/contact)'
        }
        
        print(f"Querying Wikidata for {query_desc} (language: {self.language})...")
        
        try:
            response = requests.get(
                self.sparql_endpoint,
                params={'query': query, 'format': 'json'},
                headers=headers
            )
            response.raise_for_status()
            
            data = response.json()
            entities = []
            
            for binding in data['results']['bindings']:
                entity_uri = binding['item']['value']
                entity_id = entity_uri.split('/')[-1]  # Extract Q-ID
                
                entity = {
                    'id': entity_id,
                    'uri': entity_uri,
                    'label': binding.get('itemLabel', {}).get('value', ''),
                    'description': binding.get('itemDescription', {}).get('value', '')
                }
                entities.append(entity)
            
            print(f"Retrieved {len(entities)} entities")
            return entities
            
        except Exception as e:
            print(f"Error querying Wikidata: {e}")
            return []
    
    def add_popularity_scores(self, entities: List[Dict]) -> List[Dict]:
        """Add QRank popularity scores to entities."""
        if not self.qrank_data:
            self.qrank_data = self.load_qrank_data()
        
        scored_entities = []
        
        for entity in entities:
            entity_id = entity['id']
            popularity_score = self.qrank_data.get(entity_id, 0.0)
            
            entity_with_score = entity.copy()
            entity_with_score['popularity_score'] = popularity_score
            entity_with_score['is_popular'] = popularity_score > 100000  # Threshold for "popular"
            
            scored_entities.append(entity_with_score)
        
        # Sort by popularity score (ascending = unpopular first)
        scored_entities.sort(key=lambda x: x['popularity_score'])
        
        return scored_entities
    
    def collect_entities(self, categories: List[Tuple[str, str]], limit_per_category: int = 500) -> pd.DataFrame:
        """
        Collect entities from multiple categories with popularity scoring.
        
        Args:
            categories: List of (category_name, wikidata_id) tuples
            limit_per_category: Max entities per category
        """
        all_entities = []
        
        for category_name, wikidata_id in categories:
            id_display = wikidata_id if wikidata_id else "No Type Constraint"
            print(f"\n--- Processing category: {category_name} ({id_display}) ---")
            
            entities = self.query_wikidata_entities(wikidata_id, limit_per_category)
            
            if entities:
                scored_entities = self.add_popularity_scores(entities)
                
                # Add category information
                for entity in scored_entities:
                    entity['category'] = category_name
                    entity['category_id'] = wikidata_id
                
                all_entities.extend(scored_entities)
                
                print(f"Added {len(scored_entities)} entities from {category_name}")
                
                # Be nice to the API
                time.sleep(1)
        
        # Convert to DataFrame
        df = pd.DataFrame(all_entities)
        
        print(f"\nTotal entities collected: {len(df)}")
        
        if not df.empty and 'popularity_score' in df.columns:
            print(f"Entities with popularity scores: {len(df[df['popularity_score'] > 0])}")
            print(f"Popular entities (score > 100000): {len(df[df['is_popular']])}")
        else:
            print("No entities collected or popularity scores not available")
        
        return df
    
    def save_results(self, df: pd.DataFrame, filename: str = "wikidata_entities_with_popularity.csv"):
        """Save results to CSV file."""
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"Results saved to {filename}")
        
        # Also save unpopular entities separately
        unpopular_df = df[~df['is_popular']].copy()
        unpopular_filename = filename.replace('.csv', '_unpopular.csv')
        unpopular_df.to_csv(unpopular_filename, index=False, encoding='utf-8')
        print(f"Unpopular entities saved to {unpopular_filename}")


def main():
    """Main function to demonstrate entity collection."""
    # Test both English and Chinese with local QRank CSV
    collectors = {
        'en': WikidataEntityCollector(language='en', qrank_csv_file='qrank.csv'),
        'zh': WikidataEntityCollector(language='zh', qrank_csv_file='qrank.csv')
    }
    
    # Expanded categories including places, events, works, and organizations
    categories = [
        # People
        ("Fictional Characters", "Q95074"),   # Fictional character
        ("Musicians", "Q639669"),             # Musician
        ("Politicians", "Q82955"),            # Politician
        ("Athletes", "Q2066131"),             # Athlete
        ("Scientists", "Q901"),               # Scientist
        ("Actors", "Q33999"),                 # Actor
        ("Writers", "Q36180"),                # Writer
        
        # Works and Creative Content
        ("Books", "Q571"),                    # Book
        ("Films", "Q11424"),                  # Film
        ("TV Series", "Q5398426"),            # Television series
        ("Songs", "Q7366"),                   # Song
        ("Albums", "Q482994"),                # Album
        ("Video Games", "Q7889"),             # Video game
        ("Artworks", "Q838948"),              # Work of art
        
        # Places
        ("Cities", "Q515"),                   # City
        ("Countries", "Q6256"),               # Country
        ("Universities", "Q3918"),            # University
        ("Museums", "Q33506"),                # Museum
        ("Restaurants", "Q11707"),            # Restaurant
        ("Hotels", "Q27686"),                 # Hotel
        
        # Organizations
        ("Companies", "Q783794"),             # Company
        ("NGOs", "Q79913"),                   # Non-governmental organization
        ("Sports Teams", "Q12973014"),        # Sports team
        ("Bands", "Q215380"),                 # Musical group
        ("Political Parties", "Q7278"),       # Political party
        
        # Events
        ("Wars", "Q198"),                     # War
        ("Competitions", "Q476300"),          # Competition
        ("Festivals", "Q132241"),             # Festival
        ("Conferences", "Q2020153"),          # Academic conference
        
        # Miscellaneous
        ("Awards", "Q618779"),                # Award
        ("Languages", "Q34770"),              # Language
        ("Diseases", "Q12136"),               # Disease
        ("Software", "Q7397"),                # Software
        
        # Experimental: No type constraint
        ("Mixed Entities (No Type)", None),   # No P31 constraint
    ]

    categories = categories[-2:-1]
    
    # Collect entities for both languages
    for lang_code, collector in collectors.items():
        print(f"\n{'='*60}")
        print(f"Starting collection for {lang_code.upper()} entities...")
        print(f"{'='*60}")
        
        # Use fewer categories for initial test
        test_categories = categories[:10] if lang_code == 'zh' else categories
        df = collector.collect_entities(test_categories, limit_per_category=50)
        
        # Save results with language suffix
        filename = f"wikidata_entities_with_popularity_{lang_code}.csv"
        collector.save_results(df, filename)
        
        # Display statistics
        print(f"\n=== {lang_code.upper()} SUMMARY STATISTICS ===")
        print(f"Total entities: {len(df)}")
        print("\nBy category:")
        print(df['category'].value_counts())
        
        print("\nPopularity distribution:")
        print(f"No popularity score (score = 0): {len(df[df['popularity_score'] == 0])}")
        print(f"Low popularity (0 < score <= 100000): {len(df[(df['popularity_score'] > 0) & (df['popularity_score'] <= 100000)])}")
        print(f"High popularity (score > 100000): {len(df[df['popularity_score'] > 100000])}")
        
        print(f"\nSample {lang_code} entities:")
        sample = df.head(5)
        for _, entity in sample.iterrows():
            print(f"- {entity['label']} ({entity['id']}) - Score: {entity['popularity_score']:.1f} - {entity['category']}")
            
    # Test the experimental "no type constraint" functionality
    print(f"\n{'='*60}")
    print("EXPERIMENTAL: Testing entities without type constraints...")
    print(f"{'='*60}")
    experimental_collector = WikidataEntityCollector(language='en', qrank_csv_file='qrank.csv')
    experimental_categories = [("Mixed Entities (No Type)", None)]
    experimental_df = experimental_collector.collect_entities(experimental_categories, limit_per_category=10)
    
    if not experimental_df.empty:
        print(f"Retrieved {len(experimental_df)} entities without type constraint")
        print("\nSample mixed entities:")
        sample_mixed = experimental_df.head(10)
        for _, entity in sample_mixed.iterrows():
            print(f"- {entity['label']} ({entity['id']}) - Score: {entity['popularity_score']:.1f}")
        
        experimental_collector.save_results(experimental_df, "wikidata_entities_mixed_types.csv")


if __name__ == "__main__":
    main()