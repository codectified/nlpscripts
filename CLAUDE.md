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
- Data has been normalized to new `n_root` property
- Original radical ordering logic was correct

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