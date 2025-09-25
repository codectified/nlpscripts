# Lane's Lexicon Fine-Grained Extraction Strategy

**Date**: 2024-09-17
**Based on**: SQL Database Analysis of 47,919 entries

## 🎯 Extraction Targets

Based on comprehensive analysis, we can extract these high-value semantic elements:

### 1. **Quranic Citations** 📖
- **Markers**: `Kur`, chapter/verse patterns (`iii. 9`, `[xxiv. 44]`)
- **Structure**: Often in brackets with Arabic text + English translation
- **Example**: `[in the Kur iii. 9 &c.] means...`

### 2. **Poetry & Verse References** 🎭
- **Markers**: `verse of [poet name]`, `poetry`, `metre`, poet attributions
- **Structure**: Often mentions specific poets (Imra-el-Keys, etc.)
- **Example**: `in a verse of Imra-el-Keys, where it is thus for the sake of the metre`

### 3. **Proverbs & Sayings** 🗣️
- **Markers**: `proverb`, `saying`, `maxim`, Arabic `مثل`, `قولهم`
- **Structure**: Often introduced with "It is a proverb" or similar
- **Example**: Referenced but need more samples for patterns

### 4. **Contrary/Tropical Significations** 🔄
- **Markers**: `tropical`, `contrary`, `contr.`, `metaphor`, `figurative`
- **Structure**: Both as `<sense type="tropical">` AND inline text markers
- **Example**: `(assumed tropical:)` or `tropical signification`

### 5. **Sense Hierarchy** 🏗️
- **Primary**: Main definition (no marker or `-A1-`)
- **Secondary**: `-b2-`, `-b3-`, `-b4-` (related meanings)
- **Alternative**: `-A2-`, `-A3-` (different/contrary meanings)
- **Structure**: Both XML `<sense>` elements AND inline text markers

## 🔧 Technical Implementation Strategy

### Phase 1: SQL → Neo4j ETL with XML Parsing

```python
# 1. Query SQL Database by Root/Letter
SELECT id, root, word, bword, xml FROM entry
WHERE root = 'ع-ي-ن' ORDER BY nodenum

# 2. Parse XML Structure
from xml.etree import ElementTree as ET
root_elem = ET.fromstring(xml_content)

# 3. Extract Sense Hierarchy
senses = []
for sense_elem in root_elem.findall('.//sense'):
    sense_data = {
        'sense_id': sense_elem.get('n', 'primary'),
        'sense_type': sense_elem.get('type', 'standard'),
        'definition_text': sense_elem.text or ""
    }
    senses.append(sense_data)

# 4. Extract Special Citations (Regex + XML)
citations = extract_citations(xml_content)
```

### Phase 2: Regex Patterns for Fine-Grained Extraction

#### A. Quranic Citation Extractor
```python
def extract_quranic_citations(text):
    patterns = [
        r'\\[in the Kur ([^\\]]+)\\]',           # [in the Kur iii. 9]
        r'in the Kur \\[([^\\]]+)\\]',          # in the Kur [xxiv. 44]
        r'Kur-án called ([^:]+)',               # Kur-án called الحواميم
        r'(ch\\. [ivxlc]+\\. \\d+)',            # ch. iii. 9
    ]

    citations = []
    for pattern in patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            citations.append({
                'type': 'quranic',
                'reference': match.group(1),
                'context': text[match.start()-50:match.end()+50]
            })
    return citations
```

#### B. Poetry Citation Extractor
```python
def extract_poetry_citations(text):
    patterns = [
        r'in a verse of ([^,\\.]+)',            # in a verse of Imra-el-Keys
        r'([A-Z][a-z]+(?:-[a-z]+-[A-Z][a-z]+)*) says?',  # Poet attribution
        r'for the sake of the metre',           # Metrical consideration
        r'verse[^.]*poet[^.]*',                 # General verse attribution
    ]

    poetry_refs = []
    for pattern in patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            poetry_refs.append({
                'type': 'poetry',
                'attribution': match.group(1) if match.groups() else None,
                'context': text[match.start()-100:match.end()+100]
            })
    return poetry_refs
```

#### C. Proverb & Saying Extractor
```python
def extract_proverbs(text):
    patterns = [
        r'[Ii]t is a proverb[^.]*',
        r'[Aa] proverb[^.]*',
        r'[Pp]roverbial[^.]*',
        r'مثل[^.]*',                            # Arabic "mathal"
        r'قولهم[^.]*',                          # "their saying"
    ]

    proverbs = []
    for pattern in patterns:
        matches = re.finditer(pattern, text)
        for match in matches:
            proverbs.append({
                'type': 'proverb',
                'text': match.group(0),
                'context': text[match.start()-50:match.end()+100]
            })
    return proverbs
```

#### D. Signification Type Classifier
```python
def classify_signification(sense_elem, text):
    """Determine if sense is tropical, contrary, literal, etc."""

    # Check XML attributes first
    sense_type = sense_elem.get('type', 'standard')
    if sense_type in ['tropical', 'contrary']:
        return sense_type

    # Check text patterns
    if any(marker in text.lower() for marker in ['tropical', 'trop.']):
        return 'tropical'
    elif any(marker in text.lower() for marker in ['contrary', 'contr.']):
        return 'contrary'
    elif any(marker in text.lower() for marker in ['metaphor', 'figurative']):
        return 'metaphorical'
    else:
        return 'literal'
```

## 🏛️ Proposed Neo4j Schema

```cypher
// Core lexical structure
(:Root)-[:HAS_WORD]->(:Word)-[:HAS_SENSE]->(:Sense)

// Citations and references
(:Sense)-[:CITES_QURAN]->(:QuranicCitation)
(:Sense)-[:CITES_POETRY]->(:PoetryCitation)
(:Sense)-[:CONTAINS_PROVERB]->(:Proverb)

// Sense relationships
(:Sense)-[:TROPICAL_OF]->(:Sense)
(:Sense)-[:CONTRARY_TO]->(:Sense)
(:Sense)-[:RELATED_TO]->(:Sense)

// Properties
Sense {
  sense_id: String,           // 'primary', 'b2', 'A2'
  sense_type: String,         // 'literal', 'tropical', 'contrary'
  definition_text: String,
  signification: String       // 'primary', 'secondary', 'alternative'
}

QuranicCitation {
  reference: String,          // 'iii. 9', 'xxiv. 44'
  arabic_text: String,        // Original Arabic
  translation: String,        // English translation
  context: String
}

PoetryCitation {
  poet: String,              // 'Imra-el-Keys'
  attribution: String,       // Attribution context
  verse_text: String,        // If available
  metrical_note: String      // Metre/prosody notes
}
```

## 📊 Expected Results

### Quantitative Estimates
- **Total Entries**: 47,919
- **Quranic Citations**: ~500-1000 (est. 1-2% of entries)
- **Poetry References**: ~200-500 (est. 0.5-1% of entries)
- **Proverbs**: ~100-300 (est. 0.2-0.5% of entries)
- **Tropical/Contrary Senses**: ~2000-5000 (est. 5-10% of entries)
- **Total Senses**: ~150,000-200,000 (avg. 3-4 per entry)

### Qualitative Benefits
1. **Semantic Richness**: Distinguish literal vs. metaphorical meanings
2. **Cultural Context**: Preserve Quranic and poetic connections
3. **Linguistic Analysis**: Track sense evolution and relationships
4. **Research Applications**: Enable specialized queries for Islamic studies, poetry analysis

## 🚀 Implementation Timeline

### Phase 1 (1-2 weeks): Core ETL
- SQL → Neo4j entry migration with basic sense extraction
- XML parsing for sense hierarchy (-b2-, -A2- markers)

### Phase 2 (1 week): Citation Extraction
- Quranic citation regex patterns + validation
- Poetry attribution extraction

### Phase 3 (1 week): Semantic Classification
- Tropical/contrary signification detection
- Proverb and saying extraction

### Phase 4 (1 week): Validation & Refinement
- Manual review of extracted patterns
- Regex refinement based on findings
- Performance optimization

**This strategy leverages the SQL database's structured access while using targeted regex for the fine-grained semantic extraction you want to achieve.**