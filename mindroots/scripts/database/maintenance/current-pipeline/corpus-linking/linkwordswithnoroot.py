import re
import unicodedata
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import time
import logging

load_dotenv()

# Setup dual logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('../logs/linkquranwords_phase5.log')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASS')

driver = GraphDatabase.driver(uri, auth=(user, password))


def link_items_no_root(tx):
    matched = 0
    created = 0
    failed = 0

    try:
        # Pull batch of CorpusItems without roots
        result = tx.run("""
            MATCH (ci:CorpusItem)
            WHERE ci.corpus_id = 2 
              AND ci.root IS NULL 
              AND ci.lemma IS NOT NULL
              AND NOT (ci)-[:HAS_WORD]->(:Word)
              AND ci.link_failed IS NULL
            RETURN ci.item_id AS item_id,
                   ci.lemma AS lemma,
                   ci.full_arabic_no_diac AS full_arabic_no_diac,
                   ci.lemma_norm AS lemma_norm,
                   ci.lemma_no_fem AS lemma_no_fem
            LIMIT 50
        """)

        items = list(result)
        if not items:
            logger.info("No more unlinked items without roots - processing complete")
            return 0

        logger.info(f"Found {len(items)} unlinked items without roots in this batch")

        for record in items:
            item_id = record['item_id']
            lemma = record['lemma']
            full_arabic_no_diac = record['full_arabic_no_diac']
            lemma_norm = record['lemma_norm']
            lemma_no_fem = record['lemma_no_fem']

            logger.info(
                f"Processing item {item_id}: lemma='{lemma}', "
                f"full_arabic_no_diac='{full_arabic_no_diac}', "
                f"lemma_norm='{lemma_norm}', lemma_no_fem='{lemma_no_fem}'"
            )

            word_match = None
            match_type = None

            # Layer 1: Direct surface form match
            if full_arabic_no_diac:
                word_match = tx.run("""
                    MATCH (w:Word)
                    WHERE w.arabic_no_diacritics = $full_arabic_no_diac
                    RETURN w LIMIT 1
                """, full_arabic_no_diac=full_arabic_no_diac).single()
                if word_match:
                    match_type = "surface_form"

            # Layer 2: Normalized lemma against all word layers
            if not word_match and lemma_norm:
                word_match = tx.run("""
                    MATCH (w:Word)
                    WHERE w.arabic_no_diacritics = $lemma_norm
                       OR w.arabic_normalized = $lemma_norm
                       OR w.arabic_no_fem = $lemma_norm
                    RETURN w LIMIT 1
                """, lemma_norm=lemma_norm).single()
                if word_match:
                    match_type = "normalized"

            # Layer 3: Feminine-stripped lemma against all word layers
            if not word_match and lemma_no_fem:
                word_match = tx.run("""
                    MATCH (w:Word)
                    WHERE w.arabic_no_diacritics = $lemma_no_fem
                       OR w.arabic_normalized = $lemma_no_fem
                       OR w.arabic_no_fem = $lemma_no_fem
                    RETURN w LIMIT 1
                """, lemma_no_fem=lemma_no_fem).single()
                if word_match:
                    match_type = "no_feminine"

            if word_match:
                tx.run("""
                    MATCH (ci:CorpusItem {item_id: $item_id, corpus_id: 2})
                    MATCH (w:Word)
                    WHERE elementId(w) = $wid
                    MERGE (ci)-[:HAS_WORD]->(w)
                """, item_id=item_id, wid=word_match['w'].element_id)
                logger.info(
                    f"✅ Linked item {item_id} to Word (id: {word_match['w'].element_id}) via {match_type} match"
                )
                matched += 1
            else:
                # Create placeholder Word node for particles (items without roots)
                word_create = tx.run("""
                    CREATE (w:Word {
                        arabic: $lemma,
                        arabic_no_diacritics: $lemma_norm,
                        arabic_normalized: $lemma_norm,
                        arabic_no_fem: $lemma_no_fem,
                        generated: true,
                        node_type: "Word",
                        type: "word",
                        is_particle: true,
                        particle_note: "Corpus item without root - likely particle/function word"
                    })
                    RETURN w
                """, lemma=lemma, lemma_norm=lemma_norm, lemma_no_fem=lemma_no_fem).single()

                if word_create:
                    tx.run("""
                        MATCH (ci:CorpusItem {item_id: $item_id, corpus_id: 2})
                        MATCH (w:Word)
                        WHERE elementId(w) = $wid
                        MERGE (ci)-[:HAS_WORD]->(w)
                    """, item_id=item_id, wid=word_create['w'].element_id)
                    logger.info(f"🆕 Created particle Word for item {item_id} (id: {word_create['w'].element_id})")
                    created += 1
                else:
                    tx.run("""
                        MATCH (ci:CorpusItem {item_id: $item_id, corpus_id: 2})
                        SET ci.link_failed = true, ci.link_failed_reason = 'word_creation_failed_no_root'
                    """, item_id=item_id)
                    logger.error(f"❌ Failed to create particle Word for item {item_id}")
                    failed += 1

        logger.info(f"Batch complete - Matched: {matched}, Created: {created}, Failed: {failed}")
        return len(items)

    except Exception as e:
        logger.error(f"Database error in link_items_no_root: {e}")
        raise


def main():
    logger.info("🔵 Starting Phase 5: Lemma-only linking (no root)")
    total_processed = 0
    batch_count = 0

    try:
        with driver.session() as session:
            while True:
                batch_count += 1
                logger.info(f"Starting batch {batch_count}...")
                items_processed = session.execute_write(link_items_no_root)
                total_processed += items_processed

                if items_processed == 0:
                    logger.info("🎉 Phase 5 complete!")
                    logger.info(f"📊 Final stats:")
                    logger.info(f"  - Total batches processed: {batch_count - 1}")
                    logger.info(f"  - Total items processed: {total_processed}")
                    break

                logger.info(
                    f"Batch {batch_count} complete. Running total: {total_processed} items processed"
                )
                time.sleep(0.5)

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        logger.info(f"Processed {total_processed} items in {batch_count} batches")
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        raise
    finally:
        driver.close()


if __name__ == "__main__":
    main()