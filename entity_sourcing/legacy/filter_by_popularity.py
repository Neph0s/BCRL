#!/usr/bin/env python3
"""
Filter entities by popularity score from CSV files.

This script filters out entities with popularity_score < 100 from the collected
Wikidata entity CSV files and saves the filtered results.
"""

import pandas as pd
import argparse
import os

try:
    from opencc import OpenCC
    OPENCC_AVAILABLE = True
except ImportError:
    OPENCC_AVAILABLE = False
    print("Warning: opencc-python-reimplemented not installed. Traditional to Simplified conversion will be skipped.")
    print("Install with: pip install opencc-python-reimplemented")


def filter_entities_by_popularity(input_file: str, output_file: str = None, min_popularity: float = 100.0) -> pd.DataFrame:
    """
    Filter entities by minimum popularity score.
    
    Args:
        input_file: Path to the input CSV file
        output_file: Path to the output CSV file (if None, will add '_filtered' suffix)
        min_popularity: Minimum popularity score threshold
        
    Returns:
        Filtered DataFrame
    """
    print(f"Loading entities from {input_file}...")
    
    # Read the CSV file
    df = pd.read_csv(input_file, encoding='utf-8')
    original_count = len(df)
    
    print(f"Original entities: {original_count}")
    
    # Filter by popularity score
    if 'popularity_score' in df.columns:
        filtered_df = df[df['popularity_score'] >= min_popularity].copy()
        filtered_count = len(filtered_df)
        removed_count = original_count - filtered_count
        
        print(f"Entities with popularity >= {min_popularity}: {filtered_count}")
        print(f"Filtered out: {removed_count} entities ({removed_count/original_count*100:.1f}%)")
        
        # Show popularity distribution before and after
        print(f"\nPopularity distribution (original):")
        print(f"  No score (= 0): {len(df[df['popularity_score'] == 0])}")
        print(f"  Low (0 < score < {min_popularity}): {len(df[(df['popularity_score'] > 0) & (df['popularity_score'] < min_popularity)])}")
        print(f"  Good (>= {min_popularity}): {len(df[df['popularity_score'] >= min_popularity])}")
        
        print(f"\nPopularity distribution (filtered):")
        print(f"  Low ({min_popularity} <= score <= 100000): {len(filtered_df[(filtered_df['popularity_score'] >= min_popularity) & (filtered_df['popularity_score'] <= 100000)])}")
        print(f"  High (> 100000): {len(filtered_df[filtered_df['popularity_score'] > 100000])}")
        
    else:
        print("Warning: No 'popularity_score' column found in the data")
        filtered_df = df.copy()
    
    # Generate output filename if not provided
    if output_file is None:
        base_name = input_file.rsplit('.', 1)[0]
        output_file = f"{base_name}_filtered.csv"
    
    # Save filtered results
    filtered_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"\nFiltered results saved to: {output_file}")
    
    return filtered_df


def main():
    """Main function to filter entity files by popularity."""
    parser = argparse.ArgumentParser(description='Filter Wikidata entities by popularity score')
    parser.add_argument('--min_popularity', type=float, default=100.0,
                       help='Minimum popularity score threshold (default: 100.0)')
    parser.add_argument('--input_files', nargs='*', 
                       default=['wikidata_entities_with_popularity_en_0625.csv', 
                               'wikidata_entities_with_popularity_zh_0625.csv'],
                       help='Input CSV files to filter')
    
    args = parser.parse_args()
    
    print(f"Filtering entities with popularity score >= {args.min_popularity}")
    print("=" * 60)
    
    for input_file in args.input_files:
        if not os.path.exists(input_file):
            print(f"Warning: File {input_file} does not exist, skipping...")
            continue
            
        print(f"\nProcessing {input_file}...")
        print("-" * 40)
        
        try:
            filtered_df = filter_entities_by_popularity(input_file, min_popularity=args.min_popularity)
            
            # Show sample of filtered entities
            if not filtered_df.empty:
                print(f"\nSample filtered entities:")
                sample = filtered_df.head(3)
                for _, entity in sample.iterrows():
                    print(f"  - {entity['label']} ({entity['id']}) - Score: {entity['popularity_score']:.1f}")
            
        except Exception as e:
            print(f"Error processing {input_file}: {e}")
    
    print(f"\n{'='*60}")
    print("Filtering completed!")


if __name__ == "__main__":
    main()