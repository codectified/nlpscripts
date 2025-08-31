import re
import time
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# --- Setup ---
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")
driver = GraphDatabase.driver(uri, auth=(user, password))

# --- Cleaning function ---
def clean_lemma(text: str) -> str:
    if not text:
        return None
    # Remove ^ (long vowel marker)
    text = text.replace("^", "")
    # Replace # (Buckwalter oddity) → ئ
    text = text.replace("#", "ئ")
    return text

# --- Neo4j update batch ---
def update_batch(tx, batch_size=500):
    query = """
    MATCH (ci:CorpusItem)
    WHERE ci.corpus_id = 2
      // Only process nodes that still have dirty characters
      AND any(val IN [
        ci.s1_lemma_norm, ci.s2_lemma_norm, ci.s3_lemma_norm,
        ci.s4_lemma_norm, ci.s5_lemma_norm
      ] WHERE val =~ '.*[\\^#].*')
      // Skip nodes that have already been cleaned
      AND any(cleaned IN [
        ci.s1_lemma_cleaned, ci.s2_lemma_cleaned, ci.s3_lemma_cleaned,
        ci.s4_lemma_cleaned, ci.s5_lemma_cleaned
      ] WHERE cleaned IS NULL)
    RETURN ci.item_id AS item_id,
           ci.s1_lemma_norm AS s1, ci.s2_lemma_norm AS s2,
           ci.s3_lemma_norm AS s3, ci.s4_lemma_norm AS s4,
           ci.s5_lemma_norm AS s5
    LIMIT $batch_size
    """
    result = tx.run(query, batch_size=batch_size)

    updates = []
    for record in result:
        item_id = record["item_id"]
        cleaned = {}
        for i in range(1, 5+1):
            val = record.get(f"s{i}")
            if val and ("^" in val or "#" in val):  # only clean dirty values
                cleaned[f"s{i}_lemma_cleaned"] = clean_lemma(val)
        if cleaned:
            updates.append((item_id, cleaned))

    for item_id, cleaned in updates:
        set_clause = ", ".join([f"ci.`{k}` = ${k}" for k in cleaned.keys()])
        params = {"item_id": item_id, **cleaned}
        tx.run(f"""
            MATCH (ci:CorpusItem {{corpus_id: 2, item_id: $item_id}})
            SET {set_clause}
        """, **params)

    return len(updates)

# --- Main ---
def main():
    print("🔵 Starting cleanup of sX_lemma_norm → sX_lemma_cleaned...")
    total = 0
    batch = 0
    try:
        while True:
            with driver.session() as session:
                updated = session.execute_write(update_batch)
                if updated == 0:
                    break
                total += updated
                batch += 1
                print(f"✅ Batch {batch}: Updated {updated} items (Total: {total})")
                time.sleep(0.2)  # Aura throttle
    finally:
        driver.close()
    print(f"🎉 Done. Cleaned {total} CorpusItems.")

if __name__ == "__main__":
    main()