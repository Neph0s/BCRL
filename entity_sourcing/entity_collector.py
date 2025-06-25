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
    def __init__(self, qrank_csv_file='qrank.csv', major_sample_size=2000, minor_sample_size=100):
        self.sparql_endpoint = "https://query.wikidata.org/sparql"
        self.qrank_data = {}
        self.qrank_loader = QRankLoader(csv_file=qrank_csv_file)
        self.major_sample_size = major_sample_size
        self.minor_sample_size = minor_sample_size
        
    def load_qrank_data(self) -> Dict[str, float]:
        """Load QRank popularity data using optimized loader."""
        return self.qrank_loader.load_qrank_data()
    
    def query_wikidata_entities(self, instance_of: str = None, limit: int = 1000, max_retries: int = 3) -> List[Dict]:
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
        
        # For broader coverage, we'll get entities first, then filter for labels
        query = f"""
        SELECT DISTINCT ?item WHERE {{
          {type_constraint}
        }}
        LIMIT {limit * 3}
        """
        
        headers = {
            'Accept': 'application/sparql-results+json',
            'User-Agent': 'EntityCollector/1.0 (https://example.com/contact)'
        }
        
        print(f"Querying Wikidata for {query_desc}...")
        
        for attempt in range(max_retries):
            try:
                # Add longer timeout for large queries
                timeout = 60 if limit > 500 else 60
                
                response = requests.get(
                    self.sparql_endpoint,
                    params={'query': query, 'format': 'json'},
                    headers=headers,
                    timeout=timeout
                )
                response.raise_for_status()
                
                data = response.json()
                entity_ids = []
                
                # Extract entity IDs
                for binding in data['results']['bindings']:
                    entity_uri = binding['item']['value']
                    entity_id = entity_uri.split('/')[-1]  # Extract Q-ID
                    entity_ids.append(entity_id)
                
                # Now get labels and descriptions for these entities
                english_entities, chinese_entities = self._get_entity_details(entity_ids[:limit])
                
                print(f"Retrieved {len(english_entities)} English entities, {len(chinese_entities)} Chinese entities")
                return english_entities, chinese_entities
                
            except requests.exceptions.Timeout:
                print(f"  Timeout on attempt {attempt + 1}/{max_retries}, reducing limit...")
                # Reduce limit on timeout and retry
                limit = max(100, limit // 2)
                query = f"""
                SELECT DISTINCT ?item WHERE {{
                  {type_constraint}
                }}
                LIMIT {limit * 3}
                """
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                    
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 504:  # Gateway timeout
                    print(f"  Server timeout on attempt {attempt + 1}/{max_retries}, reducing limit...")
                    limit = max(100, limit // 2)
                    query = f"""
                    SELECT DISTINCT ?item WHERE {{
                      {type_constraint}
                    }}
                    LIMIT {limit * 3}
                    """
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                elif e.response.status_code == 429:  # Too many requests
                    print(f"  Rate limited on attempt {attempt + 1}/{max_retries}, waiting...")
                    if attempt < max_retries - 1:
                        time.sleep(5 * (attempt + 1))  # Longer wait for rate limits
                        continue
                print(f"  HTTP Error: {e}")
                
            except Exception as e:
                print(f"  Error on attempt {attempt + 1}/{max_retries}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
        
        print(f"Failed to retrieve entities after {max_retries} attempts")
        return []
    
    def _get_entity_details(self, entity_ids: List[str], max_retries: int = 2) -> Tuple[List[Dict], List[Dict]]:
        """Get labels and descriptions for a list of entity IDs with retry logic
        
        Returns:
            Tuple of (english_entities, chinese_entities)
        """
        if not entity_ids:
            return [], []
        
        # Process in batches to avoid URL length limits
        batch_size = 50
        all_english_entities = []
        all_chinese_entities = []
        
        for i in range(0, len(entity_ids), batch_size):
            batch_ids = entity_ids[i:i + batch_size]
            values_clause = " ".join([f"wd:{entity_id}" for entity_id in batch_ids])
            
            # Always get both English and Chinese data
            query = f"""
            SELECT DISTINCT ?item ?itemLabel_en ?itemDescription_en ?itemLabel_zh ?itemDescription_zh WHERE {{
              VALUES ?item {{ {values_clause} }}
              OPTIONAL {{
                ?item rdfs:label ?itemLabel_en .
                FILTER(LANG(?itemLabel_en) = "en")
              }}
              OPTIONAL {{
                ?item schema:description ?itemDescription_en .
                FILTER(LANG(?itemDescription_en) = "en")
              }}
              OPTIONAL {{
                ?item rdfs:label ?itemLabel_zh .
                FILTER(LANG(?itemLabel_zh) = "zh")
              }}
              OPTIONAL {{
                ?item schema:description ?itemDescription_zh .
                FILTER(LANG(?itemDescription_zh) = "zh")
              }}
            }}
            """
            
            headers = {
                'Accept': 'application/sparql-results+json',
                'User-Agent': 'EntityCollector/1.0 (https://example.com/contact)'
            }
            
            # Retry logic for each batch
            batch_english_entities = []
            batch_chinese_entities = []
            for attempt in range(max_retries):
                try:
                    response = requests.get(
                        self.sparql_endpoint,
                        params={'query': query, 'format': 'json'},
                        headers=headers,
                        timeout=30
                    )
                    response.raise_for_status()
                    
                    data = response.json()
                    
                    for binding in data['results']['bindings']:
                        entity_uri = binding['item']['value']
                        entity_id = entity_uri.split('/')[-1]
                        
                        # Get English data
                        en_label = binding.get('itemLabel_en', {}).get('value', '')
                        en_description = binding.get('itemDescription_en', {}).get('value', '')
                        
                        # Get Chinese data
                        zh_label = binding.get('itemLabel_zh', {}).get('value', '')
                        zh_description = binding.get('itemDescription_zh', {}).get('value', '')
                        
                        # Create English entity if has English content
                        if en_label or en_description:
                            english_entity = {
                                'id': entity_id,
                                'uri': entity_uri,
                                'label': en_label or entity_id,
                                'description': en_description,
                                'language': 'en'
                            }
                            batch_english_entities.append(english_entity)
                        
                        # Create Chinese entity if has Chinese content
                        if zh_label or zh_description:
                            chinese_entity = {
                                'id': entity_id,
                                'uri': entity_uri,
                                'label': zh_label or entity_id,
                                'description': zh_description,
                                'language': 'zh'
                            }
                            batch_chinese_entities.append(chinese_entity)
                    
                    break  # Success, exit retry loop
                    
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"  Retry {attempt + 1} for entity details batch...")
                        time.sleep(1)
                        continue
                    else:
                        print(f"  Failed to get entity details for batch: {e}")
                        # Fallback: return basic entities with IDs only for English
                        batch_english_entities = [
                            {'id': eid, 'uri': f'http://www.wikidata.org/entity/{eid}', 'label': eid, 'description': '', 'language': 'en'} 
                            for eid in batch_ids
                        ]
            
            all_english_entities.extend(batch_english_entities)
            all_chinese_entities.extend(batch_chinese_entities)
            
            # Brief pause between batches
            if i + batch_size < len(entity_ids):
                time.sleep(0.5)
        
        return all_english_entities, all_chinese_entities
    
    
    def _sample_random_entities(self, limit: int) -> List[Dict]:
        """
        [LEGACY] Sample random entities from Wikidata efficiently using multiple small queries
        Note: This method is kept for backward compatibility but not recommended for new usage
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
                SELECT DISTINCT ?item ?itemLabel_en ?itemDescription_en ?itemLabel_zh ?itemDescription_zh WHERE {{
                  ?item wdt:P31 wd:{type_id} .
                  OPTIONAL {{
                    ?item rdfs:label ?itemLabel_en .
                    FILTER(LANG(?itemLabel_en) = "en")
                  }}
                  OPTIONAL {{
                    ?item schema:description ?itemDescription_en .
                    FILTER(LANG(?itemDescription_en) = "en")
                  }}
                  OPTIONAL {{
                    ?item rdfs:label ?itemLabel_zh .
                    FILTER(LANG(?itemLabel_zh) = "zh")
                  }}
                  OPTIONAL {{
                    ?item schema:description ?itemDescription_zh .
                    FILTER(LANG(?itemDescription_zh) = "zh")
                  }}
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
                    if len(all_entities) >= limit * 2:  # Account for both languages
                        break
                        
                    entity_uri = binding['item']['value']
                    entity_id = entity_uri.split('/')[-1]
                    
                    # Get English data
                    en_label = binding.get('itemLabel_en', {}).get('value', '')
                    en_description = binding.get('itemDescription_en', {}).get('value', '')
                    
                    # Get Chinese data
                    zh_label = binding.get('itemLabel_zh', {}).get('value', '')
                    zh_description = binding.get('itemDescription_zh', {}).get('value', '')
                    
                    # Create English entity if has English content
                    if en_label or en_description:
                        english_entity = {
                            'id': entity_id,
                            'uri': entity_uri,
                            'label': en_label or entity_id,
                            'description': en_description,
                            'sampled_from': type_name,
                            'language': 'en'
                        }
                        all_entities.append(english_entity)
                    
                    # Create Chinese entity if has Chinese content
                    if zh_label or zh_description:
                        chinese_entity = {
                            'id': entity_id,
                            'uri': entity_uri,
                            'label': zh_label or entity_id,
                            'description': zh_description,
                            'sampled_from': type_name,
                            'language': 'zh'
                        }
                        all_entities.append(chinese_entity)
                
                print(f"  Sampled {len(data['results']['bindings'])} {type_name}")
                
                # Small delay between queries
                time.sleep(0.5)
                
            except Exception as e:
                print(f"  Error sampling {type_name}: {e}")
                continue
        
        # Separate English and Chinese entities
        english_entities = [e for e in all_entities if e.get('language') == 'en']
        chinese_entities = [e for e in all_entities if e.get('language') == 'zh']
        
        # Shuffle for randomness
        random.shuffle(english_entities)
        random.shuffle(chinese_entities)
        
        # Trim to limit for each language
        english_entities = english_entities[:limit]
        chinese_entities = chinese_entities[:limit]
        
        print(f"Successfully sampled {len(english_entities)} English entities, {len(chinese_entities)} Chinese entities")
        return english_entities, chinese_entities
    
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
            
            scored_entities.append(entity_with_score)
        
        return scored_entities
    
    def collect_entities(self, categories: List[Tuple[str, str, str]], limit_per_category: int = 500) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Collect entities from multiple categories with popularity scoring.
        
        Args:
            categories: List of (category_name, wikidata_id, category_type) tuples
                       category_type should be 'major' or 'minor'
            limit_per_category: Max entities per category (overridden by major/minor settings)
            
        Returns:
            Tuple of (english_df, chinese_df)
        """
        all_english_entities = []
        all_chinese_entities = []
        
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
            
            if wikidata_id is None:  # Special case for random sampling
                english_entities, chinese_entities = self._sample_random_entities(sample_size)
            else:
                english_entities, chinese_entities = self.query_wikidata_entities(wikidata_id, sample_size)
            
            if english_entities:
                scored_english_entities = self.add_popularity_scores(english_entities)
                
                # Add category information
                for entity in scored_english_entities:
                    entity['category'] = category_name
                    entity['category_id'] = wikidata_id
                    entity['category_type'] = category_type
                
                all_english_entities.extend(scored_english_entities)
                print(f"Added {len(scored_english_entities)} English entities from {category_name}")
            
            if chinese_entities:
                scored_chinese_entities = self.add_popularity_scores(chinese_entities)
                
                # Add category information
                for entity in scored_chinese_entities:
                    entity['category'] = category_name
                    entity['category_id'] = wikidata_id
                    entity['category_type'] = category_type
                
                all_chinese_entities.extend(scored_chinese_entities)
                print(f"Added {len(scored_chinese_entities)} Chinese entities from {category_name}")
            
            # Be nice to the API
            time.sleep(1)
        
        # Convert to DataFrames and sort by popularity (high to low)
        english_df = pd.DataFrame(all_english_entities)
        chinese_df = pd.DataFrame(all_chinese_entities)
        
        if not english_df.empty and 'popularity_score' in english_df.columns:
            english_df = english_df.sort_values('popularity_score', ascending=False)
            
        if not chinese_df.empty and 'popularity_score' in chinese_df.columns:
            chinese_df = chinese_df.sort_values('popularity_score', ascending=False)
            
        print(f"\nTotal entities collected: {len(english_df)} English, {len(chinese_df)} Chinese")
        
        if not english_df.empty and 'popularity_score' in english_df.columns:
            print(f"English entities with popularity scores: {len(english_df[english_df['popularity_score'] > 0])}")
            print(f"Popular English entities (score > 100000): {len(english_df[english_df['popularity_score'] > 100000])}")
        
        if not chinese_df.empty and 'popularity_score' in chinese_df.columns:
            print(f"Chinese entities with popularity scores: {len(chinese_df[chinese_df['popularity_score'] > 0])}")
            print(f"Popular Chinese entities (score > 100000): {len(chinese_df[chinese_df['popularity_score'] > 100000])}")
        
        return english_df, chinese_df
    
    def save_results(self, df: pd.DataFrame, filename: str = "wikidata_entities_with_popularity.csv"):
        """Save results to CSV file, sorted by popularity (high to low)."""
        if not df.empty:
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"Results saved to {filename}")
        else:
            print(f"No data to save for {filename}")
    
    def postprocess_entities(self, input_filename: str, output_filename: str = None) -> pd.DataFrame:
        """
        Postprocess collected entities to remove duplicates and invalid labels.
        
        Args:
            input_filename: Path to the CSV file to process
            output_filename: Output filename (if None, will add '_cleaned' suffix)
        
        Returns:
            Cleaned DataFrame
        """
        print(f"Postprocessing entities from {input_filename}...")
        
        # Read the CSV file
        df = pd.read_csv(input_filename, encoding='utf-8')
        original_count = len(df)
        
        print(f"Original entities: {original_count}")
        
        # 1. Remove entities where label is just the Q-ID (invalid labels)
        invalid_labels = df['label'].str.match(r'^Q\d+$', na=False)
        invalid_count = invalid_labels.sum()
        df_cleaned = df[~invalid_labels].copy()
        
        print(f"Removed {invalid_count} entities with Q-ID labels")
        
        # 2. Remove duplicates based on entity ID (keep first occurrence)
        duplicate_mask = df_cleaned.duplicated(subset=['id'], keep='first')
        duplicate_count = duplicate_mask.sum()
        df_cleaned = df_cleaned[~duplicate_mask].copy()
        
        print(f"Removed {duplicate_count} duplicate entities")
        
        # 3. Show deduplication statistics
        if duplicate_count > 0:
            print("\nDuplicate analysis:")
            duplicates = df[df.duplicated(subset=['id'], keep=False)].sort_values('id')
            for entity_id in duplicates['id'].unique():
                entity_rows = duplicates[duplicates['id'] == entity_id]
                categories = entity_rows['category'].tolist()
                label = entity_rows['label'].iloc[0]
                print(f"  {entity_id} ({label}): found in {categories}")
        
        # 4. Sort by popularity (high to low)
        if 'popularity_score' in df_cleaned.columns:
            df_cleaned = df_cleaned.sort_values('popularity_score', ascending=False)
        
        # 5. Generate output filename if not provided
        if output_filename is None:
            base_name = input_filename.rsplit('.', 1)[0]
            output_filename = f"{base_name}_cleaned.csv"
        
        # 6. Save cleaned results
        df_cleaned.to_csv(output_filename, index=False, encoding='utf-8')
        
        final_count = len(df_cleaned)
        print(f"\nCleaning completed:")
        print(f"  Original: {original_count} entities")
        print(f"  Final: {final_count} entities")
        print(f"  Removed: {original_count - final_count} entities ({((original_count - final_count) / original_count * 100):.1f}%)")
        print(f"  Saved to: {output_filename}")
        
        return df_cleaned


def main():
    """Main function to demonstrate entity collection with major/minor categories."""
    # Single collector that gets both English and Chinese data
    collector = WikidataEntityCollector(qrank_csv_file='qrank.csv', major_sample_size=5000, minor_sample_size=500)
    
    # Enhanced categories with major/minor classification
    categories = [
        # People (Major category - high diversity and research value)
        ("People - Humans", "Q5", "major"),                      # Human
        ("People - Fictional Characters", "Q95074", "major"),    # Fictional character
        ("People - Musicians", "Q639669", "minor"),              # Musician
        ("People - Politicians", "Q82955", "minor"),             # Politician
        ("People - Athletes", "Q2066131", "minor"),              # Athlete
        ("People - Scientists", "Q901", "minor"),                # Scientist
        ("People - Actors", "Q33999", "minor"),                  # Actor
        ("People - Writers", "Q36180", "minor"),                 # Writer

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
        #("Organizations - Companies", "Q783794", "major"),       # Company
        #("Organizations - NGOs", "Q79913", "minor"),             # Non-governmental organization
        ("Organizations - Sports Teams", "Q12973014", "minor"),  # Sports team
        ("Organizations - Bands", "Q215380", "minor"),           # Musical group
        ("Organizations - Schools", "Q3914", "minor"),           # School
        #("Organizations - Associations", "Q4438121", "minor"),   # Association
        
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
    
    # Collect entities for both languages simultaneously
    print(f"\n{'='*60}")
    print(f"Starting collection for both English and Chinese entities...")
    print(f"{'='*60}")
    
    english_df, chinese_df = collector.collect_entities(categories)
    
    # Save results with language suffix
    en_filename = "wikidata_entities_with_popularity_en_0625.csv"
    zh_filename = "wikidata_entities_with_popularity_zh_0625.csv"
    
    collector.save_results(english_df, en_filename)
    collector.save_results(chinese_df, zh_filename)
    
    collector.postprocess_entities(en_filename, en_filename)
    collector.postprocess_entities(zh_filename, zh_filename)
    
    # Display statistics for English
    print(f"\n=== ENGLISH SUMMARY STATISTICS ===")
    print(f"Total entities: {len(english_df)}")
    if not english_df.empty:
        print("\nBy category:")
        print(english_df['category'].value_counts())
        
        print("\nPopularity distribution:")
        print(f"No popularity score (score = 0): {len(english_df[english_df['popularity_score'] == 0])}")
        print(f"Low popularity (0 < score <= 100000): {len(english_df[(english_df['popularity_score'] > 0) & (english_df['popularity_score'] <= 100000)])}")
        print(f"High popularity (score > 100000): {len(english_df[english_df['popularity_score'] > 100000])}")
        
        print(f"\nSample English entities:")
        sample = english_df.head(5)
        for _, entity in sample.iterrows():
            print(f"- {entity['label']} ({entity['id']}) - Score: {entity['popularity_score']:.1f} - {entity['category']}")
    
    # Display statistics for Chinese
    print(f"\n=== CHINESE SUMMARY STATISTICS ===")
    print(f"Total entities: {len(chinese_df)}")
    if not chinese_df.empty:
        print("\nBy category:")
        print(chinese_df['category'].value_counts())
        
        print("\nPopularity distribution:")
        print(f"No popularity score (score = 0): {len(chinese_df[chinese_df['popularity_score'] == 0])}")
        print(f"Low popularity (0 < score <= 100000): {len(chinese_df[(chinese_df['popularity_score'] > 0) & (chinese_df['popularity_score'] <= 100000)])}")
        print(f"High popularity (score > 100000): {len(chinese_df[chinese_df['popularity_score'] > 100000])}")
        
        print(f"\nSample Chinese entities:")
        sample = chinese_df.head(5)
        for _, entity in sample.iterrows():
            print(f"- {entity['label']} ({entity['id']}) - Score: {entity['popularity_score']:.1f} - {entity['category']}")


if __name__ == "__main__":
    main()