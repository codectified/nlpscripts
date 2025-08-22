import re
import unicodedata
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import logging

load_dotenv()

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
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

def backfill_normalization(tx):
    """Backfill arabic_normalized property for existing Word nodes"""
    updated = 0
    skipped = 0
    
    try:
        # Get Word nodes that don't have arabic_normalized property yet
        result = tx.run("""
            MATCH (w:Word)
            WHERE w.arabic_normalized IS NULL AND w.arabic IS NOT NULL
            RETURN w.arabic AS arabic, elementId(w) AS word_id
            LIMIT 100
        """)
        
        words = list(result)
        if not words:
            logger.info("No more Word nodes need normalization - backfill complete")
            return 0
            
        logger.info(f"Found {len(words)} Word nodes to normalize in this batch")
        
        for record in words:
            arabic = record['arabic']
            word_id = record['word_id']
            normalized = normalize_arabic(arabic)
            
            if normalized:
                tx.run("""
                    MATCH (w:Word)
                    WHERE elementId(w) = $word_id
                    SET w.arabic_normalized = $normalized
                """, word_id=word_id, normalized=normalized)
                logger.debug(f"✅ Updated Word {word_id}: '{arabic}' -> '{normalized}'")
                updated += 1
            else:
                logger.warning(f"⚠️ Could not normalize Word {word_id}: '{arabic}'")
                skipped += 1
        
        logger.info(f"Batch complete - Updated: {updated}, Skipped: {skipped}")
        return len(words)
        
    except Exception as e:
        logger.error(f"Database error in backfill_normalization: {e}")
        raise

def main():
    logger.info("Starting Word node normalization backfill...")
    
    total_processed = 0
    batch_count = 0
    
    try:
        with driver.session() as session:
            while True:
                batch_count += 1
                logger.info(f"Starting batch {batch_count}...")
                
                words_processed = session.execute_write(backfill_normalization)
                total_processed += words_processed
                
                if words_processed == 0:
                    logger.info(f"Backfill complete!")
                    logger.info(f"Final statistics:")
                    logger.info(f"  - Total batches processed: {batch_count - 1}")
                    logger.info(f"  - Total Word nodes updated: {total_processed}")
                    break
                
                logger.info(f"Batch {batch_count} complete. Running total: {total_processed} Word nodes processed")
                
    except KeyboardInterrupt:
        logger.info(f"Process interrupted by user")
        logger.info(f"Statistics at interruption:")
        logger.info(f"  - Batches processed: {batch_count}")
        logger.info(f"  - Total Word nodes processed: {total_processed}")
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        raise
    finally:
        driver.close()

if __name__ == "__main__":
    main()