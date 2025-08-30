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

BATCH_SIZE = 500
THROTTLE_DELAY = 0.2

# --- Main Update Function ---
def update_corpusitems(tx, batch_size):
    query = """
    MATCH (ci:CorpusItem)
    WHERE ci.corpus_id = 2
      AND (ci.arabic_concat IS NULL OR ci.extraneous IS NULL)
    RETURN elementId(ci) AS eid,
           ci.s1_arabic AS s1, ci.s2_arabic AS s2, ci.s3_arabic AS s3,
           ci.s4_arabic AS s4, ci.s5_arabic AS s5, ci.s6_arabic AS s6,
           ci.s7_arabic AS s7
    LIMIT $batch_size
    """
    result = tx.run(query, batch_size=batch_size)

    updates = []
    for record in result:
        eid = record["eid"]
        segs = [record[f"s{i}"] for i in range(1, 8) if record.get(f"s{i}")]
        arabic_concat = "".join(segs) if segs else None
        extraneous = True if record["s1"] is None else False
        updates.append((eid, arabic_concat, extraneous))

    for eid, arabic_concat, extraneous in updates:
        tx.run("""
            MATCH (ci) WHERE elementId(ci) = $eid
            SET ci.arabic_concat = $arabic_concat,
                ci.extraneous = $extraneous
        """, eid=eid, arabic_concat=arabic_concat, extraneous=extraneous)

    return len(updates)

# --- Runner ---
def main():
    print("🔵 Starting concatenation + extraneous flagging for CorpusItems...")
    total = 0
    batch = 0
    try:
        while True:
            with driver.session() as session:
                updated = session.execute_write(update_corpusitems, BATCH_SIZE)
                if updated == 0:
                    break
                total += updated
                batch += 1
                print(f"✅ Batch {batch}: Updated {updated} items (Total: {total})")
                time.sleep(THROTTLE_DELAY)
    finally:
        driver.close()
    print(f"🎉 Done. Updated {total} CorpusItems.")

if __name__ == "__main__":
    main()