import time
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import logging

# --- Setup ---
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")
driver = GraphDatabase.driver(uri, auth=(user, password))

# --- Logging ---
logger = logging.getLogger("full_arabic_clean")
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler("../logs/full_arabic_clean.log", encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(ch)

# --- Cleaning function ---
def clean_full_arabic(text: str) -> str:
    """
    Clean full_arabic text by:
    1. Removing ^ characters
    2. Replacing # with ئ
    3. Removing @ characters
    4. Collapsing whitespace
    """
    if not text:
        return None
    text = text.replace("^", "").replace("#", "ئ").replace("@", "")
    return " ".join(text.split())  # collapse whitespace

# --- Neo4j update batch ---
def update_batch(tx, batch_size=500):
    query = """
    MATCH (ci:CorpusItem)
    WHERE ci.corpus_id = 2 
      AND ci.full_arabic IS NOT NULL
      AND (ci.full_arabic CONTAINS '^' 
           OR ci.full_arabic CONTAINS '#' 
           OR ci.full_arabic CONTAINS '@' 
           OR ci.full_arabic CONTAINS '  ')
    RETURN ci.item_id AS item_id,
           ci.full_arabic AS full_arabic
    LIMIT $batch_size
    """
    result = tx.run(query, batch_size=batch_size)

    updates = []
    for record in result:
        item_id = record["item_id"]
        original_text = record["full_arabic"]
        
        cleaned_text = clean_full_arabic(original_text)
        
        if cleaned_text != original_text:
            updates.append((item_id, original_text, cleaned_text))
        else:
            logger.debug(f"⚠️ No change needed for item {item_id}")

    # Apply updates
    for item_id, original, cleaned in updates:
        tx.run("""
            MATCH (ci:CorpusItem {corpus_id: 2, item_id: $item_id})
            SET ci.full_arabic = $cleaned_text
        """, item_id=item_id, cleaned_text=cleaned)
        logger.info(f"✅ Updated item {item_id}: '{original}' → '{cleaned}'")

    return len(updates)

# --- Main loop ---
def main():
    logger.info("🔵 Starting full_arabic cleaning (Corpus 2)...")
    total = 0
    batch = 0

    try:
        while True:
            with driver.session() as session:
                updated = session.execute_write(update_batch)
                if updated == 0:
                    logger.warning("⚠️ No more updates found. Stopping.")
                    break
                total += updated
                batch += 1
                logger.info(f"Batch {batch}: {updated} items updated (Total: {total})")
                time.sleep(0.2)  # Aura safety
    finally:
        driver.close()

    logger.info(f"🎉 Done. Cleaned full_arabic for {total} CorpusItems.")

if __name__ == "__main__":
    main()