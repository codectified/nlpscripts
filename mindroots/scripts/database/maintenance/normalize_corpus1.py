"""
Normalize Corpus 1 (99 Names) Arabic Properties
===============================================

Removes diacritics from the 'arabic' property on CorpusItem nodes (corpus_id: 1)
and creates normalized property: 'arabic_no_diacritics'

Uses unified normalization module for consistency with other pipeline components.
"""

import os
import re
import sys
import time
import logging
import unicodedata
from datetime import datetime
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Configure logging
log_filename = f"normalize_corpus1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import unified normalization module
sys.path.append('/Users/omaribrahim/dev/scripts/mindroots/scripts/database/maintenance/current-pipeline')
from utilities.unified_normalization import strip_diacritics

load_dotenv()
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
)

# Throttling constants
BATCH_SIZE = 100
THROTTLE_DELAY = 0.2  # seconds between batches


def normalize_corpus1_arabic(tx, batch_size=BATCH_SIZE):
    """Fetch and normalize Corpus 1 Arabic properties in batches"""
    query = """
    MATCH (ci:CorpusItem {corpus_id: 1})
    WHERE ci.arabic IS NOT NULL AND ci.arabic_no_diacritics IS NULL
    RETURN elementId(ci) AS cid, ci.arabic AS arabic
    LIMIT $batch_size
    """
    rows = list(tx.run(query, batch_size=batch_size))
    updates = []

    for row in rows:
        cid = row["cid"]
        arabic = row["arabic"]

        if arabic:
            # Strip diacritics using unified normalization function
            arabic_no_diacritics = strip_diacritics(arabic)
            updates.append((cid, arabic, arabic_no_diacritics))

    # Apply updates
    for cid, original, normalized in updates:
        tx.run("""
            MATCH (ci:CorpusItem) WHERE elementId(ci) = $cid
            SET ci.arabic_no_diacritics = $arabic_no_diacritics
        """, cid=cid, arabic_no_diacritics=normalized)
        logger.debug(f"Updated: {original} → {normalized}")

    return len(updates)


def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("CORPUS 1 (99 NAMES) ARABIC NORMALIZATION STARTED")
    logger.info("=" * 60)

    try:
        # Get total count first
        with driver.session() as session:
            result = session.run("""
                MATCH (ci:CorpusItem {corpus_id: 1})
                WHERE ci.arabic IS NOT NULL AND ci.arabic_no_diacritics IS NULL
                RETURN count(ci) AS total
            """)
            total_to_process = result.single()["total"]
            logger.info(f"Found {total_to_process} CorpusItem nodes to normalize")

        if total_to_process == 0:
            logger.info("No nodes to process. Exiting.")
            return

        total_updated = 0

        while True:
            with driver.session() as session:
                batch_count = session.execute_write(normalize_corpus1_arabic, BATCH_SIZE)

                if batch_count == 0:
                    break

                total_updated += batch_count
                progress_pct = (total_updated / total_to_process) * 100 if total_to_process > 0 else 0
                logger.info(f"Progress: {total_updated}/{total_to_process} ({progress_pct:.1f}%) - Last batch: {batch_count} items")
                time.sleep(THROTTLE_DELAY)

        logger.info("=" * 60)
        logger.info("CORPUS 1 NORMALIZATION COMPLETED")
        logger.info(f"Total normalized: {total_updated} CorpusItem nodes")
        logger.info(f"Log saved to: {log_filename}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}", exc_info=True)
        raise
    finally:
        logger.info("Closing database connection...")
        driver.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
