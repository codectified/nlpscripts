import re
import unicodedata
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import time
import logging

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASS')

driver = GraphDatabase.driver(uri, auth=(user, password))

def strip_diacritics(text):
    arabic_diacritics = re.compile(r'[\u064B-\u0652\u0653-\u0655]')
    text = unicodedata.normalize('NFKD', text)
    return arabic_diacritics.sub('', text)

def link_items(tx):
    # Statistics for this batch
    matched = 0
    created = 0
    failed = 0
    
    try:
        # Pull a batch of CorpusItems that have a root and no existing link
        result = tx.run("""
            MATCH (ci:CorpusItem)
            WHERE ci.corpus_id = 2 AND ci.root IS NOT NULL
              AND NOT (ci)-[:HAS_WORD]->(:Word)
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
            root = record['root']
            lemma = record['lemma']
            lemma_no_diacritics = strip_diacritics(lemma)

            logger.info(f"Processing item {item_id}: lemma='{lemma}' -> '{lemma_no_diacritics}', root='{root}'")

            # Validate root exists first
            root_check = tx.run("MATCH (r:Root {arabic: $root}) RETURN r", root=root).single()
            if not root_check:
                logger.warning(f"❌ Root '{root}' not found for item {item_id}")
                failed += 1
                continue

            # Try to find existing word node under root
            word_match = tx.run("""
                MATCH (r:Root {arabic: $root})-[:HAS_WORD]->(w:Word)
                WHERE w.arabic_no_diacritics = $lemma_no_diacritics
                RETURN w LIMIT 1
            """, root=root, lemma_no_diacritics=lemma_no_diacritics).single()

            if word_match:
                tx.run("""
                    MATCH (ci:CorpusItem {item_id: $item_id})
                    MATCH (w:Word)
                    WHERE id(w) = $wid
                    MERGE (ci)-[:HAS_WORD]->(w)
                """, item_id=item_id, wid=word_match['w'].id)
                logger.info(f"✅ Linked item {item_id} to existing Word (id: {word_match['w'].id})")
                matched += 1
            else:
                # Create new Word node under that root
                word_create = tx.run("""
                    MATCH (r:Root {arabic: $root})
                    CREATE (w:Word {
                        arabic: $lemma,
                        arabic_no_diacritics: $lemma_no_diacritics,
                        generated: true,
                        node_type: "Word",
                        type: "word"
                    })
                    CREATE (r)-[:HAS_WORD]->(w)
                    RETURN w
                """, root=root, lemma=lemma, lemma_no_diacritics=lemma_no_diacritics).single()

                if word_create:
                    tx.run("""
                        MATCH (ci:CorpusItem {item_id: $item_id})
                        MATCH (w:Word)
                        WHERE id(w) = $wid
                        MERGE (ci)-[:HAS_WORD]->(w)
                    """, item_id=item_id, wid=word_create['w'].id)
                    logger.info(f"🆕 Created and linked item {item_id} to new Word (id: {word_create['w'].id})")
                    created += 1
                else:
                    logger.error(f"❌ Failed to create word for item {item_id}")
                    failed += 1
        
        logger.info(f"Batch complete - Matched: {matched}, Created: {created}, Failed: {failed}")
        return len(items)
        
    except Exception as e:
        logger.error(f"Database error in link_items: {e}")
        raise

def main():
    logger.info("Starting corpus item linking process...")
    
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
                    logger.info(f"Processing complete!")
                    logger.info(f"Final statistics:")
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