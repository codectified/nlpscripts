import os
import time
import logging
from neo4j import GraphDatabase
from dotenv import load_dotenv
from rich.console import Console
from camel_tools.utils.charmap import CharMapper
from camel_tools.utils.transliterate import Transliterator

# --- Setup ---
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")

console = Console()
driver = GraphDatabase.driver(uri, auth=(user, password))

# --- Logging ---
logging.basicConfig(
    filename="segment_conversion.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8"
)

# --- Transliterator ---
bw2ar = Transliterator(CharMapper.builtin_mapper('bw2ar'))

# --- Main conversion ---
def convert_segments():
    with driver.session() as session:
        # Fetch all Corpus 2 nodes
        result = session.run("""
            MATCH (ci:CorpusItem {corpus_id: 2})
            RETURN id(ci) AS node_id, properties(ci) AS props
        """)

        total = 0
        for record in result:
            node_id = record["node_id"]
            props = record["props"]

            updates = {}
            for i in range(1, 8):
                form_key = f"s{i}_FORM"
                arabic_key = f"s{i}_arabic"
                if form_key in props and props[form_key]:
                    try:
                        updates[arabic_key] = bw2ar.transliterate(props[form_key])
                    except Exception as e:
                        console.log(f"[red]Error converting {form_key}={props[form_key]}: {e}")
                        logging.error(f"Error converting {form_key}={props[form_key]}: {e}")

            if updates:
                session.run(
                    f"""
                    MATCH (ci)
                    WHERE id(ci) = $id
                    SET {", ".join([f"ci.`{k}` = ${k}" for k in updates])}
                    """,
                    id=node_id,
                    **updates
                )
                total += 1
                log_msg = f"Updated node {node_id} with {len(updates)} Arabic segments."
                console.log(log_msg)
                logging.info(log_msg)

            time.sleep(0.05)  # Throttle for Aura safety

        summary_msg = f"✅ Conversion complete. Updated {total} nodes."
        console.log(summary_msg)
        logging.info(summary_msg)

if __name__ == "__main__":
    try:
        convert_segments()
    finally:
        driver.close()