import csv
import time
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import logging

# === Setup Logging ===
logging.basicConfig(
    filename='update_itype.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# === Load environment variables ===
load_dotenv()
uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASS')

# === Load CSV ===
csv_path = "lanes_filtered.csv"  # Update this path if needed
df = pd.read_csv(csv_path)
df = df.dropna(subset=["nodeid", "itype"])

# === Neo4j Driver Setup ===
driver = GraphDatabase.driver(uri, auth=(user, password))

def update_itype(tx, nodeid, itype):
    result = tx.run(
        """
        MATCH (w:Word {entry_id: $nodeid})
        SET w.itype = $itype
        RETURN count(w) AS updated
        """,
        nodeid=nodeid,
        itype=itype
    )
    return result.single()["updated"]

# === Processing ===
with driver.session() as session:
    for i, row in df.iterrows():
        nodeid = row["nodeid"]
        itype = row["itype"]
        try:
            updated_count = session.write_transaction(update_itype, nodeid, itype)
            if updated_count:
                logging.info(f"Updated node {nodeid} with itype: {itype}")
            else:
                logging.warning(f"No node found for nodeid: {nodeid}")
        except Exception as e:
            logging.error(f"Failed to update node {nodeid}: {str(e)}")

        time.sleep(0.25)  # Throttle: 4 updates per second

driver.close()
logging.info("Update script complete.")