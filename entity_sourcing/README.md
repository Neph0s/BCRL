# Wikidata Entity Collector

Collect Wikidata entities with popularity ranking using QRank data.

## Quick Start

1. **Install dependencies:**
```bash
pip install -r requirements_entity_collector.txt
```

2. **Download QRank data:**
```bash
wget https://qrank.toolforge.org/download/qrank.csv.gz
gunzip qrank.csv.gz
```

3. **Run example:**
```bash
python example.py
```

## Example Code

```python
from entity_collector import WikidataEntityCollector

# Create collector
collector = WikidataEntityCollector(
    language='zh',              # Chinese entities
    major_sample_size=100,      # Major categories: 100 entities each  
    minor_sample_size=30        # Minor categories: 30 entities each
)

# Define categories
categories = [
    ("People - Fictional Characters", "Q95074", "major"),
    ("Works - Books", "Q571", "major"),
    ("Places - Cities", "Q515", "major"), 
    ("Nature - Fish", "Q152", "minor"),
    ("Events - Festivals", "Q132241", "minor"),
]

# Collect and save (sorted by popularity)
df = collector.collect_entities(categories)
collector.save_results(df, "entities.csv")

# Clean data (remove duplicates and invalid labels)
df_cleaned = collector.postprocess_entities("entities.csv", "entities_cleaned.csv")
```

## Data Cleaning

After collection, use postprocessing to clean the data:

```bash
python postprocess.py entities.csv
```

This removes:
- **Invalid labels**: Entities where label is just the Q-ID (e.g., "Q55664413")
- **Duplicates**: Same entity appearing in multiple categories (keeps first occurrence)

## Category Types

- **Major**: High research value, gets more samples (e.g., People, Works, Places)
- **Minor**: Specialized categories, gets fewer samples (e.g., specific animals, events)

## Common Categories

### Major Categories (Large samples)
| Category | Wikidata ID | Description |
|----------|-------------|-------------|
| Humans | Q5 | Real people |
| Fictional Characters | Q95074 | Novel/TV characters |
| Books | Q571 | Published books |
| Films | Q11424 | Movies |
| Songs | Q7366 | Music tracks |
| Cities | Q515 | World cities |
| Countries | Q6256 | Nations |
| Companies | Q783794 | Business organizations |
| Sports Teams | Q12973014 | Athletic teams |

### Minor Categories (Smaller samples)
| Category | Wikidata ID | Description |
|----------|-------------|-------------|
| Musicians | Q639669 | Music artists |
| Scientists | Q901 | Researchers |
| Fish | Q152 | Fish species |
| Museums | Q33506 | Cultural institutions |
| Festivals | Q132241 | Cultural events |

## Output Format

CSV file with entities sorted by popularity (high to low):
- `id`: Wikidata ID (Q12345)
- `label`: Entity name
- `description`: Entity description
- `popularity_score`: QRank score (0 to millions)
- `category`: Category name
- `category_type`: major/minor

## Languages

Support English (`en`) and Chinese (`zh`). Easily extensible to other languages.

## Files

- `entity_collector.py` - Main collection engine
- `qrank_loader.py` - QRank data loader with caching
- `example.py` - Simple usage example
- `demo_usage.py` - Random sampling demo (legacy)
- `qrank.csv` - Download separately (2.4GB)
- `qrank_cache.pkl` - Auto-generated cache