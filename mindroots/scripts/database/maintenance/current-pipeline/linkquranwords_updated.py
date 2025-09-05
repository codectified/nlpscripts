import re
import unicodedata
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import time
import logging

load_dotenv()

# Setup dual logging (file and console)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# File handler
file_handler = logging.FileHandler('../logs/linkquranwords_updated.log')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASS')

driver = GraphDatabase.driver(uri, auth=(user, password))

def strip_diacritics(text):
    if text is None:
        return None
    arabic_diacritics = re.compile(r'[\u064B-\u0652\u0653-\u0655]')
    text = unicodedata.normalize('NFKD', text)
    return arabic_diacritics.sub('', text)

def normalize_arabic(text):
    """
    Normalize Arabic text for better matching by handling orthographic variants.
    This function:
    1. Removes diacritics
    2. Normalizes alif variants (إ, أ, آ → ا)
    3. Normalizes ya variants (ى → ي)
    4. Normalizes ta marbuta variants (ة → ه)
    """
    if text is None:
        return None
    
    # First strip diacritics
    text = strip_diacritics(text)
    
    # Normalize orthographic variants
    # Alif variants: أ (hamza above), إ (hamza below), آ (madda) → ا (plain alif)
    text = re.sub(r'[أإآ]', 'ا', text)
    
    # Ya variants: ى (alif maqsura) → ي (ya)
    text = re.sub(r'ى', 'ي', text)
    
    # Ta marbuta: ة → ه (convert to ha for more consistent matching)
    text = re.sub(r'ة', 'ه', text)
    
    return text

def link_items(tx):
    # Statistics for this batch
    matched = 0
    created = 0
    failed = 0
    
    try:
        # Pull a batch of CorpusItems that have a root, lemma, no existing link, and haven't failed linking
        # **UPDATED**: Now uses the cleaned lemma property from our pipeline
        result = tx.run("""
            MATCH (ci:CorpusItem)
            WHERE ci.corpus_id = 2 AND ci.root IS NOT NULL AND ci.lemma IS NOT NULL
              AND NOT (ci)-[:HAS_WORD]->(:Word)
              AND ci.link_failed IS NULL
            RETURN ci.item_id AS item_id, ci.root AS root, ci.lemma AS lemma
            LIMIT 50
        """)
        
        items = list(result)
        if not items:
            logger.info("No more unlinked items found - processing complete")
            return 0
            
        logger.info(f"Found {len(items)} unlinked items in this batch")
        
        for record in items:
            item_id = record['item_id']
            corpus_root = record['root']  # This is plain format (e.g., 'بني')
            lemma = record['lemma']
            lemma_no_diacritics = strip_diacritics(lemma)
            lemma_normalized = normalize_arabic(lemma)

            logger.info(f"Processing item {item_id}: lemma='{lemma}' -> no_diacritics='{lemma_no_diacritics}', normalized='{lemma_normalized}', root='{corpus_root}'")

            # **UPDATED**: Validate root exists using the new plain_root property
            logger.debug(f"🔍 Searching for root with plain_root: '{corpus_root}'")
            root_check = tx.run("""
                MATCH (r:Root) 
                WHERE r.plain_root = $corpus_root
                RETURN r
            """, corpus_root=corpus_root).single()
            
            if not root_check:
                logger.warning(f"❌ Root with plain_root '{corpus_root}' not found for item {item_id}")
                # Mark this item as failed to avoid retrying it
                tx.run("""
                    MATCH (ci:CorpusItem {item_id: $item_id, corpus_id: 2})
                    SET ci.link_failed = true, ci.link_failed_reason = 'root_not_found'
                """, item_id=item_id)
                failed += 1
                continue
            else:
                logger.debug(f"✅ Found root with plain_root: '{corpus_root}'")

            # **UPDATED**: Try to find existing word node under root using plain_root matching
            logger.debug(f"🔍 Searching for word no_diacritics='{lemma_no_diacritics}' or normalized='{lemma_normalized}' under root with plain_root '{corpus_root}'")
            word_match = tx.run("""
                MATCH (r:Root)-[:HAS_WORD]->(w:Word)
                WHERE r.plain_root = $corpus_root
                  AND (w.arabic_no_diacritics = $lemma_no_diacritics
                       OR w.arabic_normalized = $lemma_normalized)
                RETURN w LIMIT 1
            """, corpus_root=corpus_root, lemma_no_diacritics=lemma_no_diacritics, lemma_normalized=lemma_normalized).single()

            if word_match:
                tx.run("""
                    MATCH (ci:CorpusItem {item_id: $item_id, corpus_id: 2})
                    MATCH (w:Word)
                    WHERE elementId(w) = $wid
                    MERGE (ci)-[:HAS_WORD]->(w)
                """, item_id=item_id, wid=word_match['w'].element_id)
                logger.info(f"✅ Linked item {item_id} to existing Word (id: {word_match['w'].element_id})")
                matched += 1
            else:
                # **UPDATED**: Create new Word node under root using plain_root matching
                word_create = tx.run("""
                    MATCH (r:Root)
                    WHERE r.plain_root = $corpus_root
                    CREATE (w:Word {
                        arabic: $lemma,
                        arabic_no_diacritics: $lemma_no_diacritics,
                        arabic_normalized: $lemma_normalized,
                        generated: true,
                        node_type: "Word",
                        type: "word"
                    })
                    CREATE (r)-[:HAS_WORD]->(w)
                    RETURN w
                """, corpus_root=corpus_root, lemma=lemma, lemma_no_diacritics=lemma_no_diacritics, lemma_normalized=lemma_normalized).single()

                if word_create:
                    tx.run("""
                        MATCH (ci:CorpusItem {item_id: $item_id, corpus_id: 2})
                        MATCH (w:Word)
                        WHERE elementId(w) = $wid
                        MERGE (ci)-[:HAS_WORD]->(w)
                    """, item_id=item_id, wid=word_create['w'].element_id)
                    logger.info(f"🆕 Created and linked item {item_id} to new Word (id: {word_create['w'].element_id})")
                    created += 1
                else:
                    logger.error(f"❌ Failed to create word for item {item_id}")
                    # Mark this item as failed to avoid retrying it
                    tx.run("""
                        MATCH (ci:CorpusItem {item_id: $item_id, corpus_id: 2})
                        SET ci.link_failed = true, ci.link_failed_reason = 'word_creation_failed'
                    """, item_id=item_id)
                    failed += 1
        
        logger.info(f"Batch complete - Matched: {matched}, Created: {created}, Failed: {failed}")
        return len(items)
        
    except Exception as e:
        logger.error(f"Database error in link_items: {e}")
        raise

def main():
    logger.info("🔵 Starting Phase 4: Corpus item linking to Lane's Lexicon...")
    logger.info("📋 Using cleaned lemma properties and plain_root matching")
    
    total_processed = 0
    batch_count = 0
    
    try:
        with driver.session() as session:
            while True:
                batch_count += 1
                logger.info(f"Starting batch {batch_count}...")
                
                items_processed = session.execute_write(link_items)
                total_processed += items_processed
                
                if items_processed == 0:
                    logger.info(f"🎉 Phase 4 linking complete!")
                    logger.info(f"📊 Final statistics:")
                    logger.info(f"  - Total batches processed: {batch_count - 1}")
                    logger.info(f"  - Total items processed: {total_processed}")
                    break
                
                logger.info(f"Batch {batch_count} complete. Running total: {total_processed} items processed")
                time.sleep(0.5)  # Avoid DB throttling
                
    except KeyboardInterrupt:
        logger.info(f"Process interrupted by user")
        logger.info(f"Statistics at interruption:")
        logger.info(f"  - Batches processed: {batch_count}")
        logger.info(f"  - Total items processed: {total_processed}")
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        raise
    finally:
        driver.close()

if __name__ == "__main__":
    main()