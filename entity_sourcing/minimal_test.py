#!/usr/bin/env python3
"""
Minimal test to verify SPARQL queries work
"""

import requests

def test_sparql_query(language='en', instance_of='Q515', limit=3):
    """Test a single SPARQL query"""
    sparql_endpoint = "https://query.wikidata.org/sparql"
    
    if instance_of:
        type_constraint = f"?item wdt:P31 wd:{instance_of} ."
        query_desc = f"instances of {instance_of}"
    else:
        type_constraint = "?item wdt:P31 ?type ."
        query_desc = "entities without type constraint"
    
    query = f"""
    SELECT DISTINCT ?item ?itemLabel ?itemDescription WHERE {{
      {type_constraint}
      ?item rdfs:label ?itemLabel .
      FILTER(LANG(?itemLabel) = "{language}")
      OPTIONAL {{ ?item schema:description ?itemDescription . FILTER(LANG(?itemDescription) = "{language}") }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{language}" }}
    }}
    LIMIT {limit}
    """
    
    headers = {
        'Accept': 'application/sparql-results+json',
        'User-Agent': 'EntityCollector/1.0 (https://example.com/contact)'
    }
    
    print(f"Testing {query_desc} in {language}...")
    
    try:
        response = requests.get(
            sparql_endpoint,
            params={'query': query, 'format': 'json'},
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        entities = []
        
        for binding in data['results']['bindings']:
            entity_uri = binding['item']['value']
            entity_id = entity_uri.split('/')[-1]
            
            entity = {
                'id': entity_id,
                'uri': entity_uri,
                'label': binding.get('itemLabel', {}).get('value', ''),
                'description': binding.get('itemDescription', {}).get('value', '')
            }
            entities.append(entity)
        
        print(f"✓ Found {len(entities)} entities")
        for entity in entities:
            print(f"  - {entity['label']} ({entity['id']})")
        
        return entities
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return []

def main():
    """Test various query types"""
    print("Testing enhanced SPARQL queries...\n")
    
    # Test 1: English cities
    test_sparql_query('en', 'Q515', 3)
    
    print()
    
    # Test 2: Chinese cities  
    test_sparql_query('zh', 'Q515', 3)
    
    print()
    
    # Test 3: Universities in English
    test_sparql_query('en', 'Q3918', 3)
    
    print()
    
    # Test 4: Untyped entities (any entity with a type)
    test_sparql_query('en', None, 3)
    
    print("\nAll tests completed!")

if __name__ == "__main__":
    main()