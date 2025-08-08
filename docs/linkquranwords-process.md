# Corpus-to-Lexicon Linking Process

## Overview

The `linkquranwords.py` script implements a sophisticated linking process that connects Arabic corpus items (from Quranic text) to lexical word nodes in the Neo4j knowledge graph.

## Process Flow

### 1. Data Preparation
- **Source**: CorpusItem nodes with `corpus_id = 2` (Quranic corpus)
- **Requirements**: Items must have `n_root` property and no existing `[:HAS_WORD]` relationship
- **Batch Size**: 50 items per batch for optimal performance

### 2. Root Validation
```cypher
MATCH (r:Root {arabic: $root}) RETURN r
```
- Validates that the normalized root (`n_root`) exists in the Root collection
- Fails gracefully if root is missing
- Logs validation results for debugging

### 3. Word Matching Strategy

#### Lemma Normalization
- Strips Arabic diacritics using Unicode patterns: `[\u064B-\u0652\u0653-\u0655]`
- Applies NFKD Unicode normalization
- Creates `lemma_no_diacritics` for matching

#### Word Search Query
```cypher
MATCH (r:Root {arabic: $root})-[:HAS_WORD]->(w:Word)
WHERE w.arabic_no_diacritics = $lemma_no_diacritics
RETURN w LIMIT 1
```
- Searches for existing word nodes under the validated root
- Matches against `arabic_no_diacritics` property
- Root context ensures accuracy (prevents cross-root matches)

### 4. Linking or Creation

#### Case 1: Existing Word Found
```cypher
MATCH (ci:CorpusItem {item_id: $item_id})
MATCH (w:Word) WHERE id(w) = $wid
MERGE (ci)-[:HAS_WORD]->(w)
```
- Creates `HAS_WORD` relationship to existing word
- Logs successful link with word ID

#### Case 2: New Word Creation
```cypher
MATCH (r:Root {arabic: $root})
CREATE (w:Word {
    arabic: $lemma,
    arabic_no_diacritics: $lemma_no_diacritics,
    generated: true,
    node_type: "Word",
    type: "word"
})
CREATE (r)-[:HAS_WORD]->(w)
```
- Creates new Word node under the correct root
- Marks as `generated: true` for provenance tracking
- Establishes both root-word and corpus-word relationships

## Logging & Monitoring

### Log Levels
- **INFO**: Batch progress, successful operations
- **DEBUG**: Individual query execution details  
- **WARNING**: Missing roots, validation failures
- **ERROR**: Database errors, creation failures

### Output Destinations
- **File**: `linkquranwords.log` (persistent record)
- **Console**: Real-time progress monitoring
- **Format**: `%(asctime)s - %(levelname)s - %(message)s`

### Statistics Tracking
Per batch:
- `matched`: Linked to existing words
- `created`: New words generated  
- `failed`: Items that couldn't be processed

Global totals:
- Total batches processed
- Total items processed
- Success/failure rates

## Error Handling

### Root Validation Failures
- **Cause**: `n_root` value not found in Root collection
- **Action**: Skip item, log warning, increment failure counter
- **Impact**: Prevents orphaned word creation

### Database Connection Issues
- **Recovery**: Transaction rollback and connection retry
- **Logging**: Detailed error messages with context
- **Graceful Exit**: Statistics preserved on interruption

### Batch Processing Resilience
- **Interruption**: Ctrl+C handling with final statistics
- **Throttling**: 0.5s delay between batches
- **Memory**: Connection pooling and cleanup

## Performance Characteristics

### Throughput
- **Target**: ~50 items per batch
- **Rate**: Approximately 100-200 items/minute
- **Bottlenecks**: Root validation queries, word creation

### Optimization Strategies
- Batch size tuning based on database performance
- Connection pooling for reduced overhead
- Index utilization on `n_root` and `arabic_no_diacritics`

## Data Quality Assurance

### Matching Accuracy
- Root-scoped word searches prevent cross-contamination
- Diacritic normalization ensures consistent matching
- Generated flag tracks provenance of created nodes

### Validation Steps
1. Root existence verification
2. Lemma normalization validation  
3. Duplicate relationship prevention
4. Statistics verification

## Recent Updates (2025-01-08)

### Root Property Migration
- **Changed**: `ci.root` → `ci.n_root` in queries
- **Reason**: Data normalization resolved orthography inconsistencies
- **Impact**: Improved matching accuracy, reduced false negatives

### Enhanced Logging
- **Added**: Query-level debugging logs
- **Improved**: Root search and word matching visibility
- **Format**: Dual output with structured formatting

### Error Recovery
- **Enhanced**: Transaction-level error handling
- **Added**: Graceful interruption support
- **Improved**: Statistics preservation on exit