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
    def __init__(self, language='en', qrank_csv_file='qrank.csv', major_sample_size=2000, minor_sample_size=100):
        self.sparql_endpoint = "https://query.wikidata.org/sparql"
        self.qrank_data = {}
        self.language = language  # Support for different languages
        self.qrank_loader = QRankLoader(csv_file=qrank_csv_file)
        self.major_sample_size = major_sample_size
        self.minor_sample_size = minor_sample_size
        
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
            # For random sampling without type constraint, use offset-based sampling
            # This avoids complex UNION queries that timeout
            return self._sample_random_entities(limit)
        
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
    
    def _sample_random_entities(self, limit: int) -> List[Dict]:
        """
        Sample random entities from Wikidata efficiently using multiple small queries
        """
        import random
        
        print(f"Sampling {limit} random entities from multiple categories...")
        
        # List of common entity types for sampling
        sample_types = [
            ("Q515", "cities"),
            ("Q571", "books"), 
            ("Q11424", "films"),
            ("Q783794", "companies"),
            ("Q95074", "fictional characters"),
            ("Q7366", "songs"),
            ("Q3918", "universities"),
            ("Q33506", "museums"),
            ("Q215380", "musical groups"),
            ("Q5398426", "TV series"),
            ("Q7889", "video games"),
            ("Q482994", "albums"),
            ("Q838948", "artworks"),
            ("Q132241", "festivals"),
            ("Q618779", "awards")
        ]
        
        all_entities = []
        entities_per_type = max(1, (limit // len(sample_types)) + 1)
        
        # Shuffle types for randomness
        random.shuffle(sample_types)
        
        for type_id, type_name in sample_types:
            if len(all_entities) >= limit:
                break
                
            try:
                # Use small queries with random offset
                offset = random.randint(0, 100)  # Random starting point
                query = f"""
                SELECT DISTINCT ?item ?itemLabel ?itemDescription WHERE {{
                  ?item wdt:P31 wd:{type_id} .
                  ?item rdfs:label ?itemLabel .
                  FILTER(LANG(?itemLabel) = "{self.language}")
                  OPTIONAL {{ ?item schema:description ?itemDescription . FILTER(LANG(?itemDescription) = "{self.language}") }}
                  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{self.language}" }}
                }}
                OFFSET {offset}
                LIMIT {entities_per_type}
                """
                
                headers = {
                    'Accept': 'application/sparql-results+json',
                    'User-Agent': 'EntityCollector/1.0 (https://example.com/contact)'
                }
                
                response = requests.get(
                    self.sparql_endpoint,
                    params={'query': query, 'format': 'json'},
                    headers=headers,
                    timeout=30
                )
                response.raise_for_status()
                
                data = response.json()
                
                for binding in data['results']['bindings']:
                    if len(all_entities) >= limit:
                        break
                        
                    entity_uri = binding['item']['value']
                    entity_id = entity_uri.split('/')[-1]
                    
                    entity = {
                        'id': entity_id,
                        'uri': entity_uri,
                        'label': binding.get('itemLabel', {}).get('value', ''),
                        'description': binding.get('itemDescription', {}).get('value', ''),
                        'sampled_from': type_name
                    }
                    all_entities.append(entity)
                
                print(f"  Sampled {len(data['results']['bindings'])} {type_name}")
                
                # Small delay between queries
                time.sleep(0.5)
                
            except Exception as e:
                print(f"  Error sampling {type_name}: {e}")
                continue
        
        # Shuffle final results for randomness
        random.shuffle(all_entities)
        
        # Trim to exact limit
        final_entities = all_entities[:limit]
        
        print(f"Successfully sampled {len(final_entities)} random entities")
        return final_entities
    
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
    
    def collect_entities(self, categories: List[Tuple[str, str, str]], limit_per_category: int = 500) -> pd.DataFrame:
        """
        Collect entities from multiple categories with popularity scoring.
        
        Args:
            categories: List of (category_name, wikidata_id, category_type) tuples
                       category_type should be 'major' or 'minor'
            limit_per_category: Max entities per category (overridden by major/minor settings)
        """
        all_entities = []
        
        for category_name, wikidata_id, category_type in categories:
            id_display = wikidata_id if wikidata_id else "No Type Constraint"
            
            # Determine sample size based on category type
            if category_type == 'major':
                sample_size = self.major_sample_size
            elif category_type == 'minor':
                sample_size = self.minor_sample_size
            else:
                sample_size = limit_per_category
            
            print(f"\n--- Processing {category_type} category: {category_name} ({id_display}) - {sample_size} entities ---")
            
            entities = self.query_wikidata_entities(wikidata_id, sample_size)
            
            if entities:
                scored_entities = self.add_popularity_scores(entities)
                
                # Add category information
                for entity in scored_entities:
                    entity['category'] = category_name
                    entity['category_id'] = wikidata_id
                    entity['category_type'] = category_type
                
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
    """Main function to demonstrate entity collection with major/minor categories."""
    # Test both English and Chinese with enhanced sampling
    collectors = {
        'en': WikidataEntityCollector(language='en', qrank_csv_file='qrank.csv', major_sample_size=100, minor_sample_size=40),
        'zh': WikidataEntityCollector(language='zh', qrank_csv_file='qrank.csv', major_sample_size=100, minor_sample_size=40)
    }
    
    # Enhanced categories with major/minor classification
    categories = [
        # People (Major category - high diversity and research value)
        ("People - Humans", "Q5", "major"),                      # Human
        ("People - Fictional Characters", "Q95074", "major"),    # Fictional character
        ("People - Historical Figures", "Q5774265", "major"),     # Historical figure
        ("People - Musicians", "Q639669", "minor"),              # Musician
        ("People - Politicians", "Q82955", "minor"),             # Politician
        ("People - Athletes", "Q2066131", "minor"),              # Athlete
        ("People - Scientists", "Q901", "minor"),                # Scientist
        ("People - Actors", "Q33999", "minor"),                  # Actor
        ("People - Writers", "Q36180", "minor"),                 # Writer
        ("People - Artists", "Q483501", "minor"),                # Artist
        ("People - Journalists", "Q1930187", "minor"),           # Journalist
        ("People - Businesspeople", "Q43845", "minor"),          # Businessperson
        ("People - Philosophers", "Q4964182", "minor"),          # Philosopher
        ("People - Inventors", "Q205375", "minor"),              # Inventor
        
        # Works and Creative Content (Major category)
        ("Works - Books", "Q571", "major"),                      # Book
        ("Works - Films", "Q11424", "major"),                    # Film
        ("Works - TV Series", "Q5398426", "major"),              # Television series
        ("Works - Songs", "Q7366", "minor"),                     # Song
        ("Works - Video Games", "Q7889", "minor"),               # Video game
        ("Works - Artworks", "Q838948", "minor"),                # Work of art
        ("Works - Plays", "Q25379", "minor"),                    # Play
        ("Works - Anime", "Q1107", "major"),                     # Anime
        ("Works - Comics", "Q1004", "minor"),                    # Comic
        
        # Places (Major category)
        ("Places - Cities", "Q515", "major"),                    # City
        ("Places - Universities", "Q3918", "minor"),             # University
        ("Places - Museums", "Q33506", "minor"),                 # Museum
        
        # Organizations (Major category)
        ("Organizations - Companies", "Q783794", "major"),       # Company
        ("Organizations - NGOs", "Q79913", "minor"),             # Non-governmental organization
        ("Organizations - Sports Teams", "Q12973014", "minor"),  # Sports team
        ("Organizations - Bands", "Q215380", "minor"),           # Musical group
        ("Organizations - Schools", "Q3914", "minor"),           # School
        ("Organizations - Associations", "Q4438121", "minor"),   # Association
        
        # Events (Minor category)
        ("Events - Wars", "Q198", "minor"),                      # War
        ("Events - Competitions", "Q476300", "minor"),           # Competition
        ("Events - Disasters", "Q3839081", "minor"),             # Disaster
        
        # # Science & Technology (Minor category)
        # ("Science - Diseases", "Q12136", "minor"),               # Disease
        # ("Science - Software", "Q7397", "minor"),                # Software
        # ("Science - Chemical Compounds", "Q11173", "minor"),     # Chemical compound
        # ("Science - Inventions", "Q15401930", "minor"),          # Invention
        # ("Science - Theories", "Q17737", "minor"),               # Theory
        # ("Science - Experiments", "Q33147", "minor"),            # Experiment
        
        # Culture & Society (Minor category)
        ("Culture - Cuisines", "Q1968435", "minor"),             # Cuisine
        
        # Experimental: Mixed sampling (Special category)
        ("Mixed Entities (No Type)", None, "major"),             # No P31 constraint
    ]

    # Use only mixed entities for demonstration
    categories = categories[-1:]
    
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