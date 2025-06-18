# Wikidata Entity Sourcing with Popularity Ranking

A comprehensive system for collecting Wikidata entities with popularity scoring using QRank data. Supports multi-language collection, major/minor category classification, and detailed biological taxonomies.

## Features

- 🌍 **Multi-language support** (English, Chinese, extensible)
- 📊 **Major/Minor classification** with flexible sampling sizes
- 🔬 **Detailed biological categories** (fish, birds, felines, plants, etc.)
- ⚡ **Optimized QRank caching** for fast repeated access
- 🎯 **Random sampling strategies** to avoid query timeouts
- 📈 **Popularity-based filtering** for research applications

## Quick Start

### 1. Installation
```bash
pip install -r requirements_entity_collector.txt
```

### 2. Download QRank Data
Download the QRank popularity dataset:
```bash
# Download and extract QRank data (required for popularity scoring)
wget https://qrank.toolforge.org/download/qrank.csv.gz
gunzip qrank.csv.gz
```

**Note**: The qrank.csv file is approximately 2.4GB and contains popularity data for 28+ million Wikidata entities. This file is required for popularity scoring but is not included in the repository due to its size.

### 3. Basic Usage
```bash
# Run demonstration with 50 random Chinese entities
python demo_usage.py
```

### 4. Advanced Usage
```python
from entity_collector import WikidataEntityCollector

# Create collector with custom sampling sizes
collector = WikidataEntityCollector(
    language='zh', 
    qrank_csv_file='qrank.csv',
    major_sample_size=2000,  # Major categories get more samples
    minor_sample_size=100    # Minor categories get fewer samples
)

# Define categories with major/minor classification
categories = [
    ("People - Fictional Characters", "Q95074", "major"),    # 2000 entities
    ("Works - Books", "Q571", "major"),                      # 2000 entities  
    ("Nature - Fish", "Q152", "major"),                      # 2000 entities
    ("Places - Museums", "Q33506", "minor"),                 # 100 entities
    ("Events - Festivals", "Q132241", "minor"),              # 100 entities
]

df = collector.collect_entities(categories)
```

## Enhanced Category System

### Major Categories (High Research Value)
**People (人物类)**
- Fictional Characters (Q95074), Musicians (Q639669), Politicians (Q82955)
- Athletes (Q2066131), Scientists (Q901), Actors (Q33999), Writers (Q36180)

**Works (作品类)**  
- Books (Q571), Films (Q11424), TV Series (Q5398426)
- Songs (Q7366), Albums (Q482994), Video Games (Q7889)

**Places (地点类)**
- Cities (Q515), Countries (Q6256), Universities (Q3918)

**Organizations (组织类)**
- Companies (Q783794), Sports Teams (Q12973014), Bands (Q215380)

**Nature (自然类)**
- Fish (Q152), Birds (Q5113), Mammals (Q7377), Insects (Q1390)
- Trees (Q10884), Flowers (Q506)

### Minor Categories (Specialized Research)
**Biological Subcategories**
- Cats/Felidae (Q25265), Dogs/Canidae (Q25324), Bears (Q33609)
- Birds of Prey (Q164509), Songbirds (Q21800), Marine Fish (Q2643239)
- Orchids (Q160117), Roses (Q102231), Medicinal Plants (Q1540899)

**Cultural & Social**
- Museums (Q33506), Restaurants (Q11707), Festivals (Q132241)
- Awards (Q618779), Languages (Q34770), Cuisines (Q1968435)

## Data Structure

Each collected entity includes:
- `id`: Wikidata entity ID (e.g., Q12345)
- `label`: Entity name/label
- `description`: Entity description  
- `popularity_score`: QRank popularity score (0 to millions)
- `is_popular`: Boolean flag (threshold: 100,000)
- `category`: Entity category name
- `category_type`: Classification type (major/minor)
- `sampled_from`: Source type (for random sampling)

## Performance & Optimization

- **First run**: Loads QRank data (~5 minutes), creates cache automatically
- **Subsequent runs**: Instant loading from cache
- **Random sampling**: Uses multiple small queries to avoid timeouts
- **Network queries**: Individual category queries complete within 30 seconds

## Multi-language Support

```python
# English entities
en_collector = WikidataEntityCollector(
    language='en', 
    qrank_csv_file='qrank.csv',
    major_sample_size=2000,
    minor_sample_size=100
)

# Chinese entities
zh_collector = WikidataEntityCollector(
    language='zh', 
    qrank_csv_file='qrank.csv', 
    major_sample_size=2000,
    minor_sample_size=100
)
```

## Research Applications

### Popularity Analysis
```python
# Filter unpopular entities for research
unpopular = df[~df['is_popular']]
print(f"Found {len(unpopular)} unpopular entities")

# Analyze popularity distribution by category
popularity_stats = df.groupby('category')['popularity_score'].agg(['mean', 'median', 'std'])
```

### Biological Diversity Studies
- Species popularity vs conservation status correlation
- Cross-taxonomic knowledge representation
- Long-tail entity discovery in biological domains

### Cultural Knowledge Analysis
- Entity popularity across different languages/cultures
- Cross-domain knowledge graph construction
- Bias analysis in cultural representation

## Core Files

- `entity_collector.py` - Main collection engine with enhanced categories
- `qrank_loader.py` - Optimized QRank data loader with caching
- `demo_usage.py` - Demonstration script for random sampling
- `qrank.csv` - QRank popularity data (download separately)
- `qrank_cache.pkl` - Auto-generated cache for fast loading

## Dependencies

```
requests>=2.25.1
pandas>=1.3.0
```

## Troubleshooting

1. **Query timeouts**: Reduce sample sizes or use random sampling mode
2. **Empty results**: Some categories may have limited entities in specific languages
3. **Cache issues**: Delete `qrank_cache.pkl` to regenerate
4. **Memory usage**: QRank cache requires ~2GB RAM for full dataset

## Technical Architecture

- **SPARQL queries** with P31 property constraints for typed entities
- **Offset-based random sampling** for large category collections  
- **Local CSV caching** with automatic validation
- **UTF-8 encoding** for international character support
- **Rate limiting** for Wikidata API courtesy

## License

This project is for research and educational purposes. Wikidata content is available under Creative Commons licensing.