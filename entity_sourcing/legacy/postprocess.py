#!/usr/bin/env python3
"""
Postprocess collected Wikidata entities to remove duplicates and invalid labels.
Usage: python postprocess.py input_file.csv [output_file.csv]
"""

import sys
from entity_collector import WikidataEntityCollector

def main():
    if len(sys.argv) < 2:
        print("Usage: python postprocess.py input_file.csv [output_file.csv]")
        print("Example: python postprocess.py collected_entities.csv")
        return
    
    input_filename = sys.argv[1]
    output_filename = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Create a collector instance for postprocessing
    collector = WikidataEntityCollector()
    
    # Process the file
    df_cleaned = collector.postprocess_entities(input_filename, output_filename)
    
    print(f"\nPostprocessing complete. Cleaned data has {len(df_cleaned)} entities.")

if __name__ == "__main__":
    main()