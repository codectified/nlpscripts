from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import pyarabic.trans as trans
from rich.console import Console
import time

# --- Setup ---
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")
driver = GraphDatabase.driver(uri, auth=(user, password))
console = Console()

# --- Config ---
# In your dataset, CorpusItem nodes only go up to s5_root (not s6 or s7).
SEGMENT_RANGE = range(1, 6)  # s1 to s5
CORPUS_ID = 2
BATCH_SIZE = 100
THROTTLE_DELAY = 0.1  # seconds

# --- Conversion ---
def buckwalter_to_arabic_spaced(bw):
    if bw:
        arabic = trans.convert(bw, 'tim', 'arabic')
        letters = list(arabic)
        return '-'.join(letters)
    return None

def update_roots(tx):
    count = 0
    query = f"""
    MATCH (ci:CorpusItem)
    WHERE ci.corpus_id = $corpus_id AND ci.root_test IS NULL
      AND ({' OR '.join([f'ci.s{i}_root IS NOT NULL' for i in SEGMENT_RANGE])})
    RETURN elementId(ci) AS eid, {{
      {', '.join([f's{i}_root: ci.s{i}_root' for i in SEGMENT_RANGE])}
    }} AS roots
    ORDER BY elementId(ci)
    LIMIT $batch_size
    """
    results = tx.run(query, corpus_id=CORPUS_ID, batch_size=BATCH_SIZE)

    for record in results:
        element_id = record["eid"]
        roots = record["roots"]

        # Pick the first non-null root (in s1 to s5 order)
        arabic_root = None
        for sx_root in roots.values():
            if sx_root:
                arabic_root = buckwalter_to_arabic_spaced(sx_root)
                break

        if not arabic_root:
            continue

        tx.run("""
        MATCH (ci) WHERE elementId(ci) = $eid
        SET ci.root_test = $root
        """, eid=element_id, root=arabic_root)

        console.log(f"[green]✔ Set root_test on node {element_id}: {arabic_root}")
        count += 1

    return count

# --- Main ---
def main():
    console.log("[blue]Starting Buckwalter → Arabic root conversion (processing all nodes)...")

    total = 0
    batch_count = 0

    try:
        while True:
            with driver.session() as session:
                updated = session.execute_write(update_roots)
                if updated == 0:
                    break

                total += updated
                batch_count += 1
                console.log(f"[cyan]Batch {batch_count}: Updated {updated} nodes (Total: {total})")

                if updated > 0:
                    time.sleep(THROTTLE_DELAY)

    except Exception as e:
        console.log(f"[red]❌ Error occurred: {e}")
        console.log(f"[yellow]Processed {total} nodes before error.")
        raise

    finally:
        driver.close()

    console.log(f"[bold green]✅ Done. Updated {total} nodes total.")

if __name__ == "__main__":
    main()