# MindRoots NLP & Data Pipeline Documentation

## Overview

This project implements an Arabic language processing and knowledge graph construction pipeline for the MindRoots system. The pipeline processes various Arabic text corpora and lexical resources to build a comprehensive Neo4j graph database of Arabic linguistic data.

## Core Components

### 1. Data Pipeline Architecture

The system processes multiple data sources into a unified Neo4j knowledge graph:

- **Corpus Data**: Quranic text with morphological annotations
- **Lexical Data**: Hans Wehr dictionary, Lane's Lexicon 
- **Root System**: Arabic triliteral/quadriliteral root classification
- **Morphological Forms**: Arabic verb patterns (wazn) and word forms

### 2. Database Schema

**Core Node Types:**
- `Root`: Arabic roots with normalized properties (`arabic`, `n_root`)
- `Word`: Lexical items linked to roots (`arabic_no_diacritics`, `root_id`)  
- `CorpusItem`: Text instances from various corpora (`corpus_id`, `lemma`, `n_root`)
- `Form`: Morphological patterns and verb forms

**Key Relationships:**
- `(Root)-[:HAS_WORD]->(Word)`: Root-to-word associations
- `(CorpusItem)-[:HAS_WORD]->(Word)`: Corpus-to-lexicon links
- `(Word)-[:HAS_FORM]->(Form)`: Morphological patterns

### 3. Processing Scripts

#### Core Linking Scripts
- `linkquranwords.py`: Links Quranic corpus items to word nodes via lemma matching
- `link99names.py`: Links the 99 Names of Allah to corresponding words
- `createcorpusnodeandlink.py`: Creates corpus entries and establishes links

#### Data Import Scripts  
- `importquran.py`: Imports Quranic text data
- `importqitems.py`: Imports corpus items
- `addHansWehr.py`: Integrates Hans Wehr dictionary data

#### Processing & Enhancement
- `batchprocess.py`: Batch processing for OpenAI API calls
- `openaibatches_*.py`: Various batch processing scripts for different data types
- `updatewordlabels.py`: Updates word classification labels

## Current Status

### Recent Improvements (2025-01-08)
- **Root Normalization**: Updated to use `n_root` property for consistent matching
- **Enhanced Logging**: Dual logging to both file and terminal with detailed query tracking
- **Batch Processing**: Optimized batch sizes and error handling
- **Statistics Tracking**: Comprehensive progress and success rate monitoring

### Active Development
- Corpus-to-lexicon linking via `linkquranwords.py`
- Root validation and word node creation
- Morphological pattern extraction and classification

## Data Quality & Normalization

### Root Matching Challenges
- **Historical Issue**: Initial orthography inconsistencies in Lane lexicon data
- **Solution**: Normalized all root representations to `n_root` property
- **Impact**: Eliminated false negatives in root-word matching

### Diacritics Handling
- Systematic stripping of Arabic diacritics for matching
- Preservation of original forms in separate properties
- Unicode normalization (NFKD) for consistent processing

## Technical Architecture

### Dependencies
- **Neo4j**: Graph database backend
- **python-dotenv**: Environment variable management  
- **OpenAI API**: Batch processing for text analysis
- **Custom utilities**: Arabic text processing and normalization

### Logging & Monitoring
- Comprehensive logging with timestamps
- Dual output (file + terminal)
- Progress tracking with statistics
- Error handling and recovery

### Batch Processing
- 50-item batches for optimal performance
- Database connection pooling
- Graceful interruption handling
- Automatic retry logic for failed operations

## Usage Patterns

### Running Scripts
```bash
cd mindroots
python linkquranwords.py    # Link corpus items to words
python importquran.py       # Import Quranic data
python batchprocess.py      # Process data via OpenAI API
```

### Environment Setup
Required environment variables:
- `NEO4J_URI`: Database connection string
- `NEO4J_USER`: Database username  
- `NEO4J_PASS`: Database password
- `OPENAI_API_KEY`: For batch processing

## Future Enhancements

### Planned Features
- Cross-corpus validation and linking
- Enhanced morphological analysis
- Semantic relationship extraction
- Performance optimization for large datasets

### Research Directions
- Multi-dialectal Arabic support
- Historical linguistic analysis
- Cross-linguistic root relationships
- Advanced NLP model integration