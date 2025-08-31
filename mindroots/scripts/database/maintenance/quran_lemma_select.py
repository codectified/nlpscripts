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
logger = logging.getLogger("lemma_seg")
logger.setLevel(logging.INFO)

fh = logging.FileHandler("lemma_seg_update.log", encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(ch)

# --- Neo4j update ---
def update_corpusitems(tx, batch_size=500):
    query = """
    MATCH (ci:CorpusItem)
    WHERE ci.corpus_id = 2 AND ci.lemma_seg IS NULL
    WITH ci LIMIT $batch_size
    SET ci.lemma_seg = coalesce(
        CASE WHEN ci.s1_root IS NOT NULL AND ci.s1_lemma_cleaned IS NOT NULL THEN ci.s1_lemma_cleaned
             WHEN ci.s1_root IS NOT NULL AND ci.s1_lemma_norm IS NOT NULL THEN ci.s1_lemma_norm END,
        CASE WHEN ci.s2_root IS NOT NULL AND ci.s2_lemma_cleaned IS NOT NULL THEN ci.s2_lemma_cleaned
             WHEN ci.s2_root IS NOT NULL AND ci.s2_lemma_norm IS NOT NULL THEN ci.s2_lemma_norm END,
        CASE WHEN ci.s3_root IS NOT NULL AND ci.s3_lemma_cleaned IS NOT NULL THEN ci.s3_lemma_cleaned
             WHEN ci.s3_root IS NOT NULL AND ci.s3_lemma_norm IS NOT NULL THEN ci.s3_lemma_norm END,
        CASE WHEN ci.s4_root IS NOT NULL AND ci.s4_lemma_cleaned IS NOT NULL THEN ci.s4_lemma_cleaned
             WHEN ci.s4_root IS NOT NULL AND ci.s4_lemma_norm IS NOT NULL THEN ci.s4_lemma_norm END,
        CASE WHEN ci.s5_root IS NOT NULL AND ci.s5_lemma_cleaned IS NOT NULL THEN ci.s5_lemma_cleaned
             WHEN ci.s5_root IS NOT NULL AND ci.s5_lemma_norm IS NOT NULL THEN ci.s5_lemma_norm END,
        CASE WHEN ci.s6_root IS NOT NULL AND ci.s6_lemma_cleaned IS NOT NULL THEN ci.s6_lemma_cleaned
             WHEN ci.s6_root IS NOT NULL AND ci.s6_lemma_norm IS NOT NULL THEN ci.s6_lemma_norm END,
        CASE WHEN ci.s7_root IS NOT NULL AND ci.s7_lemma_cleaned IS NOT NULL THEN ci.s7_lemma_cleaned
             WHEN ci.s7_root IS NOT NULL AND ci.s7_lemma_norm IS NOT NULL THEN ci.s7_lemma_norm END
    )
    RETURN count(ci) AS updated
    """
    result = tx.run(query, batch_size=batch_size).single()
    return result["updated"]

# --- Main loop ---
def main():
    logger.info("🔵 Starting lemma_seg backfill (Corpus 2)...")
    total = 0
    batch = 0

    try:
        while True:
            with driver.session() as session:
                updated = session.execute_write(update_corpusitems)
                if updated == 0:
                    break
                total += updated
                batch += 1
                logger.info(f"Batch {batch}: {updated} items updated (Total: {total})")
                time.sleep(0.2)  # throttle
    finally:
        driver.close()

    logger.info(f"🎉 Done. Updated {total} CorpusItems.")

if __name__ == "__main__":
    main()