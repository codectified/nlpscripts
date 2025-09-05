# MindRoots Scripts Project

## Overview
This project contains scripts for processing and linking Arabic corpus data with word nodes in a Neo4j database.

## Current Scripts

### linkquranwords.py
Links Corpus 2 items to Word nodes in the database using lemma matching and root filtering.

**Matching Logic:**
- Strips diacritics from corpus item lemma
- Matches against `arabic_no_diacritics` property on Word nodes
- Filters by root to ensure accuracy
- Creates new Word nodes if no match found under the correct root

**Key Features:**
- Comprehensive logging with timestamps
- Root validation before word creation
- Batch processing (50 items at a time)
- Graceful exit when processing complete
- Error handling and statistics tracking
- Proper database connection cleanup

**Example Match:**
- Corpus item: lemma="اسم", root="س-م-و" 
- Word node: arabic_no_diacritics="اسم", under root "س-م-و"

**Usage:**
```bash
cd mindroots
python linkquranwords.py
```

**Dependencies:**
- neo4j
- python-dotenv
- Environment variables: NEO4J_URI, NEO4J_USER, NEO4J_PASS

## Database Schema Notes
- CorpusItem nodes have: corpus_id, lemma, root, item_id
- Word nodes have: arabic_no_diacritics, root_id
- Root nodes have: arabic property
- Relationships: (Root)-[:HAS_WORD]->(Word), (CorpusItem)-[:HAS_WORD]->(Word)

## Recent Improvements (2025-01-08)
- Added comprehensive logging and error handling to linkquranwords.py
- Added root validation before word creation
- Implemented proper exit conditions and statistics tracking
- Enhanced batch processing with detailed progress reporting

## Recent Fixes (2025-08-08)
- **CRITICAL FIX**: Corrected property mapping in linkquranwords.py:
  - CorpusItem nodes use `ci.root` (original property)
  - Root nodes use `r.n_root` OR `r.arabic` (normalized property + fallback)
  - Fixed query to match both arabic and n_root properties on Root nodes
- Updated deprecated `id()` function calls to `elementId()` to eliminate warnings
- Script now successfully processes ~50 items per batch, linking existing words and creating new ones
- Identified that some roots need normalization (e.g., 'ع-س-ع-س' → 'ع-س-س') - separate script needed
- **CRITICAL FIX**: Added `corpus_id: 2` filter to linking queries to prevent cross-corpus contamination
- Added failed item tracking (`link_failed` property) to prevent infinite loops on persistently failing items

## Phase 2 Refactor (2025-09-05)

**Overview**: Complete refactor of the normalization and segment rebuilding pipeline to improve lemma matching accuracy between Quranic corpus and Lane's lexicon.

### Phase 2 Scripts (maintenance/current-pipeline/phase2-refactor/)

#### 1. rebuild_segments.py
**Purpose**: Rebuild Arabic segments from Buckwalter transliteration and create multiple Arabic representations.

**Key Features:**
- Converts Buckwalter forms (sX_form) to Arabic (sX_arabic) 
- Handles special Buckwalter symbols: `^` (madda), `#` (hamza on ya), `@` (long alif)
- Creates concatenated representations for matching

**Properties Created:**
- `sX_arabic`: Individual segment Arabic forms
- `full_arabic`: Complete Arabic text with diacritics  
- `full_arabic_no_diac`: Diacritics stripped only (preserves articles for Lane matching)
- `sem`: Copy of full_arabic

#### 2. normalize_lemmas.py  
**Purpose**: Normalize CorpusItem lemmas with layered normalization for flexible matching.

**Key Features:**
- Converts Buckwalter lemmas to Arabic using camel-tools
- Applies consistent Unicode normalization (NFKD + diacritic removal)
- Creates multiple normalization layers for different matching strategies

**Properties Created:**
- `lemma`: Arabic lemma (converted from Buckwalter)
- `lemma_norm`: Standard normalization (alifs, ya, hamza seats normalized)  
- `lemma_no_fem`: Conservative normalization (+ feminine markers ة removed entirely)

#### 3. normalize_words.py
**Purpose**: Normalize existing Word nodes to match CorpusItem processing.

**Key Features:**
- Applies identical normalization logic to Word nodes under Lane's lexicon
- Ensures consistent matching properties across corpus and lexicon

**Properties Created:**
- `arabic_no_diacritics`: Diacritics stripped only
- `arabic_normalized`: Standard normalization  
- `arabic_no_fem`: Conservative normalization (ة removed entirely)

### Critical Unicode Normalization Fixes
- **Madda Alif Handling**: Fixed `آ` decomposition via NFKD → `ا` + `ٓ` (madda above)
- **Hamza Alif Handling**: Fixed `أإ` decomposition → `ا` + hamza marks  
- **Extended Diacritic Range**: Pattern `[\u064B-\u0655\u0670]` includes madda (U+0653) and hamza marks (U+0654-U+0655)
- **Hamza Seat Normalization**: `ؤئ` decompose to base letters + hamza marks (automatically stripped)

### Normalization Strategy
**Layered Approach for Maximum Matching Flexibility:**
1. **Article-Preserving**: `full_arabic_no_diac` vs Lane entries with articles (الكتاب)
2. **Standard**: `lemma_norm`, `arabic_normalized` vs Lane entries (normalized forms)  
3. **Conservative**: `lemma_no_fem`, `arabic_no_fem` vs Lane entries (orthographic variants)

**Test Cases Verified:**
- `آزفة` → `ازفة` (norm) → `ازف` (no_fem) ✅
- `أإآٱسم` → `ااااسم` (all alif variants) ✅  
- `سؤال` → `سوال` (hamza seats) ✅
- `الكِتَابُ` → `الكتاب` (article preservation) ✅

### Execution Order
**Sequential execution recommended:**
1. `rebuild_segments.py` - Reconstruct Arabic segments
2. `normalize_lemmas.py` - Normalize CorpusItem lemmas  
3. `normalize_words.py` - Normalize Word nodes
4. Ready for improved linking with multiple matching strategies

## Version Control Best Practices
**Important:** Always use incremental commits and branching when working on code changes:
- Create a new branch for each feature/fix: `git checkout -b feature/description`
- Commit changes frequently with descriptive messages
- Test each increment before committing
- This prevents losing work and makes it easier to revert specific changes
- Both Claude and user should follow this practice

## Data Notes
- Root matching issues were initially thought to be letter ordering problems
- Actual issue was orthography inconsistencies in Lane lexicon data
- Data has been normalized to new `n_root` property on **Root nodes** (not CorpusItem nodes)
- Original radical ordering logic was correct
- Some roots still need normalization between corpus classifications vs Lane classifications
- Quadriliteral roots like 'ع-س-ع-س' may need mapping to triliteral doubled forms 'ع-س-س'

## Claude Code Permissions & Capabilities

### Git Operations (Auto-approved)
- `git push`, `git add`, `git commit`, `git remote set-url`
- `git restore`, `git checkout`, `git merge`, `git branch`
- Full version control operations without user approval required

### File System Operations  
- `rm` commands (file removal)
- Full read/write access to project files
- Directory creation and management

### Programming & Execution
- `python` script execution and debugging
- All development tools and interpreters
- Database connections and queries

### Documentation Standards
- Create documentation when explicitly requested by user
- Always update CLAUDE.md with new project insights and learnings
- Use TodoWrite tool consistently for task planning and progress tracking
- Maintain comprehensive logging in all processing scripts
- Document data pipeline processes and architectural decisions

## Project Documentation Structure
- `/docs/` - General NLP and data pipeline documentation
- `/docs/README.md` - Project overview and architecture
- `/docs/linkquranwords-process.md` - Detailed process documentation
- `CLAUDE.md` - Project instructions and development notes