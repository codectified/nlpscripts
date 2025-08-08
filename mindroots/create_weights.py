import csv
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# ─── Load Credentials ───────────────────────────────────────────────────────────
load_dotenv()
uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASS')

if not all([uri, user, password]):
    raise ValueError("Missing Neo4j credentials in environment.")

driver = GraphDatabase.driver(uri, auth=(user, password))

# ─── Update Function ────────────────────────────────────────────────────────────
def update_word_node(tx, entry_id, wazn, form):
    query = """
    MATCH (w:Word {entry_id: $entry_id})
    SET w.wazn = $wazn, w.form = $form
    """
    tx.run(query, entry_id=entry_id, wazn=wazn, form=form)

# ─── Batch Updater ──────────────────────────────────────────────────────────────
def update_neo4j_from_csv(csv_file_path, batch_size=100):
    with open(csv_file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        batch = []

        with driver.session() as session:
            for i, row in enumerate(reader, start=1):
                entry_id = row["entry_id_xml"]
                wazn = row["wazn"]
                form = row["form"]

                # Skip malformed or empty entries
                if wazn == "NA" or form == "NA":
                    continue

                batch.append((entry_id, wazn, form))

                if len(batch) >= batch_size:
                    session.execute_write(batch_update, batch)
                    print(f"✔ Processed batch at row {i}")
                    batch = []

            if batch:
                session.execute_write(batch_update, batch)
                print(f"✔ Final batch processed, total: {i} rows")

def batch_update(tx, batch):
    for entry_id, wazn, form in batch:
        update_word_node(tx, entry_id, wazn, form)

# ─── Entry Point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    csv_file = "compact_wazn_output.csv"  # Replace if needed
    update_neo4j_from_csv(csv_file)

    print("✅ All updates complete.")
    driver.close()