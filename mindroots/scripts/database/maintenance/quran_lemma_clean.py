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
logger = logging.getLogger("lemma_backfill_debug")
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler("lemma_backfill_debug.log", encoding="utf-8")
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
def update_batch(tx, batch_size=50):  # smaller batch for debugging
    query = """
    MATCH (ci:CorpusItem)
    WHERE ci.corpus_id = 2 AND ci.lemma IS NULL
    RETURN ci.item_id AS item_id,
           ci.s1_lemma AS s1_lemma, ci.s1_lemma_norm AS s1_lemma_norm,
           ci.s2_lemma AS s2_lemma, ci.s2_lemma_norm AS s2_lemma_norm,
           ci.s3_lemma AS s3_lemma, ci.s3_lemma_norm AS s3_lemma_norm,
           ci.s4_lemma AS s4_lemma, ci.s4_lemma_norm AS s4_lemma_norm,
           ci.s5_lemma AS s5_lemma, ci.s5_lemma_norm AS s5_lemma_norm
    LIMIT $batch_size
    """
    result = list(tx.run(query, batch_size=batch_size))

    if not result:
        logger.warning("⚠️ Query returned no results")
        return 0

    updates = []
    for idx, record in enumerate(result):
        item_id = record["item_id"]
        lemma_val = None

        logger.debug(f"Item {item_id} → s1={record.get('s1_lemma_norm')} "
                     f"s2={record.get('s2_lemma_norm')} "
                     f"s3={record.get('s3_lemma_norm')} "
                     f"s4={record.get('s4_lemma_norm')} "
                     f"s5={record.get('s5_lemma_norm')}")

        # Loop over s1–s5
        for i in range(1, 6):
            lemma_norm = record.get(f"s{i}_lemma_norm")
            lemma_raw  = record.get(f"s{i}_lemma")

            if lemma_norm:
                lemma_val = clean_lemma(lemma_norm)
                break
            elif lemma_raw:
                lemma_val = clean_lemma(lemma_raw)
                break

        if lemma_val:
            updates.append((item_id, lemma_val))
            logger.info(f"✅ Candidate update: item {item_id} → lemma='{lemma_val}'")

        if idx < 5:  # only dump first 5 records for readability
            logger.debug(f"Sample record {idx+1}: {record}")

    # Apply updates
    for item_id, lemma_val in updates:
        tx.run("""
            MATCH (ci:CorpusItem {corpus_id: 2, item_id: $item_id})
            SET ci.lemma = $lemma_val
        """, item_id=item_id, lemma_val=lemma_val)

    return len(updates)

# --- Main loop ---
def main():
    logger.info("🔵 Starting lemma backfill (Corpus 2, s1–s5, DEBUG MODE)...")
    total = 0
    batch = 0

    try:
        while batch < 3:  # just run 3 batches for inspection
            with driver.session() as session:
                updated = session.execute_write(update_batch)
                if updated == 0:
                    logger.warning("⚠️ No updates this batch, stopping early")
                    break
                total += updated
                batch += 1
                logger.info(f"Batch {batch}: {updated} items updated (Total: {total})")
                time.sleep(0.2)
    finally:
        driver.close()

    logger.info(f"🎉 Done. Backfilled lemma for {total} CorpusItems (DEBUG RUN).")

if __name__ == "__main__":
    main()