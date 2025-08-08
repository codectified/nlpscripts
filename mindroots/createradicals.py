import os
import json
from dotenv import load_dotenv
from neo4j import GraphDatabase

# ─── Load Neo4j Credentials ────────────────────────────────────────────────────
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")

if not all([uri, user, password]):
    raise EnvironmentError("Missing Neo4j credentials (NEO4J_URI, NEO4J_USER, NEO4J_PASS)")

driver = GraphDatabase.driver(uri, auth=(user, password))

# ─── Logging & Checkpoint Setup ────────────────────────────────────────────────
CHECKPOINT_FILE = "radical_checkpoint.json"
LOG_FILE = "radical_linking.log"

def log(msg):
    print(msg)
    with open(LOG_FILE, "a", encoding="utf-8") as log_file:
        log_file.write(msg + "\n")

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

def save_checkpoint(done_ids):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(list(done_ids), f)

# ─── Neo4j Transaction Logic ───────────────────────────────────────────────────
def link_radicals_to_root(tx, root_id, radicals_with_pos):
    query = """
    MATCH (root:Root) WHERE id(root) = $root_id
    UNWIND $radicals_with_pos AS pair
    MERGE (rp:RadicalPosition {radical: pair.radical, position: pair.position})
    MERGE (root)-[:HAS_RADICAL]->(rp)
    """
    tx.run(query, root_id=root_id, radicals_with_pos=radicals_with_pos)

# ─── Batch Processor ───────────────────────────────────────────────────────────
def process_all_roots(batch_size=500):
    processed_ids = load_checkpoint()

    with driver.session() as session:
        result = session.run("""
            MATCH (r:Root)
            RETURN id(r) AS id,
                   r.r1 AS r1, r.r2 AS r2, r.r3 AS r3,
                   r.r4 AS r4, r.r5 AS r5, r.r6 AS r6, r.r7 AS r7
        """)
        all_records = list(result)
        total = len(all_records)
        log(f"📦 Total Root nodes: {total}")

        for i in range(0, total, batch_size):
            batch = all_records[i:i + batch_size]
            log(f"🔁 Processing batch {i}–{i + len(batch) - 1}")

            with driver.session() as batch_session:
                for rec in batch:
                    root_id = rec["id"]
                    if root_id in processed_ids:
                        continue

                    radicals = []
                    for pos in range(1, 8):  # r1 to r7
                        radical = rec.get(f"r{pos}")
                        if radical:
                            radicals.append({"radical": radical, "position": pos})

                    if radicals:
                        try:
                            batch_session.execute_write(link_radicals_to_root, root_id, radicals)
                            processed_ids.add(root_id)
                        except Exception as e:
                            log(f"❌ Error processing root {root_id}: {e}")

            save_checkpoint(processed_ids)
            log(f"✔️ Finished batch {i // batch_size + 1} ({len(batch)} roots)")

    log("✅ All radical relationships created.")
    driver.close()

# ─── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        process_all_roots()
    finally:
        driver.close()