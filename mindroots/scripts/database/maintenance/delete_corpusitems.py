import time
import logging
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# --- Setup ---
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")
driver = GraphDatabase.driver(uri, auth=(user, password))

# --- Logging ---
logging.basicConfig(
    filename="delete_corpus2_items.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# --- Delete function ---
def delete_batch(tx, batch_size=5000):
    query = """
    MATCH (ci:CorpusItem {corpus_id: 2})
    WITH ci LIMIT $batch_size
    DETACH DELETE ci
    RETURN count(ci) AS deleted
    """
    result = tx.run(query, batch_size=batch_size).single()
    return result["deleted"]

# --- Main loop ---
def main():
    logging.info("🚨 Starting batch deletion of CorpusItem nodes for corpus_id=2...")
    total_deleted = 0
    batch_count = 0

    try:
        while True:
            with driver.session() as session:
                deleted = session.execute_write(delete_batch)
                if deleted == 0:
                    break
                total_deleted += deleted
                batch_count += 1
                logging.info(f"✅ Batch {batch_count}: Deleted {deleted} nodes (Total: {total_deleted})")
                time.sleep(0.2)  # throttle for Aura safety

    finally:
        driver.close()
        logging.info(f"🎉 Done. Deleted {total_deleted} CorpusItem nodes total.")

if __name__ == "__main__":
    main()