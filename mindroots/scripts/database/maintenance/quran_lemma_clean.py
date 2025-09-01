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
logger = logging.getLogger("lemma_backfill")
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler("lemma_backfill.log", encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(ch)

# --- Cleaning function ---
def clean_lemma(text: str) -> str:
    if not text:
        return None
    return text.replace("^", "").replace("#", "ئ")

# --- Neo4j update batch ---
def update_batch(tx, batch_size=500):
    query = """
    MATCH (ci:CorpusItem)
    WHERE ci.corpus_id = 2 
      AND ci.lemma IS NULL
      AND (ci.s1_lemma_norm IS NOT NULL 
           OR ci.s2_lemma_norm IS NOT NULL 
           OR ci.s3_lemma_norm IS NOT NULL
           OR ci.s4_lemma_norm IS NOT NULL 
           OR ci.s5_lemma_norm IS NOT NULL)
    RETURN ci.item_id AS item_id,
           ci.s1_lemma_norm AS s1_norm,
           ci.s2_lemma_norm AS s2_norm,
           ci.s3_lemma_norm AS s3_norm,
           ci.s4_lemma_norm AS s4_norm,
           ci.s5_lemma_norm AS s5_norm
    LIMIT $batch_size
    """
    result = tx.run(query, batch_size=batch_size)

    updates = []
    for record in result:
        item_id = record["item_id"]
        lemma_val = None

        # Loop over s1–s5 lemma_norms
        for i in range(1, 6):
            lemma_norm = record.get(f"s{i}_norm")
            if lemma_norm:
                lemma_val = clean_lemma(lemma_norm)
                break

        if lemma_val:
            updates.append((item_id, lemma_val))
        else:
            logger.debug(f"⚠️ No lemma_norm found for item {item_id}")

    # Apply updates
    for item_id, lemma_val in updates:
        tx.run("""
            MATCH (ci:CorpusItem {corpus_id: 2, item_id: $item_id})
            SET ci.lemma = $lemma_val
        """, item_id=item_id, lemma_val=lemma_val)
        logger.info(f"✅ Updated item {item_id} → lemma='{lemma_val}'")

    return len(updates)

# --- Main loop ---
def main():
    logger.info("🔵 Starting lemma backfill (Corpus 2, using sX_lemma_norm)...")
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

    logger.info(f"🎉 Done. Backfilled lemma for {total} CorpusItems.")

if __name__ == "__main__":
    main()