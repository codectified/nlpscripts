from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import pyarabic.trans as trans
from rich.console import Console
import time

load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")
driver = GraphDatabase.driver(uri, auth=(user, password))
console = Console()

SEGMENT_RANGE = range(1, 8)  # s1 to s7
CORPUS_ID = 2
BATCH_SIZE = 100  # Process nodes in batches
THROTTLE_DELAY = 0.1  # 100ms delay between batches for Aura throttling

def buckwalter_to_arabic_spaced(bw):
    if bw:
        arabic = trans.convert(bw, 'tim', 'arabic')
        # Split into individual letters and join with hyphens
        letters = list(arabic)
        return '-'.join(letters)
    return None

def update_roots(tx):
    count = 0
    query = f"""
    MATCH (ci:CorpusItem)
    WHERE ci.corpus_id = $corpus_id AND ci.root IS NULL
    RETURN elementId(ci) AS eid, {{ {', '.join([f's{i}_root: ci.s{i}_root' for i in SEGMENT_RANGE])} }} AS roots
    LIMIT $batch_size
    """
    results = tx.run(query, corpus_id=CORPUS_ID, batch_size=BATCH_SIZE)

    for record in results:
        element_id = record["eid"]
        roots = record["roots"]

        # Pick the first non-null root (in s1 to s7 order)
        for sx_root in roots.values():
            if sx_root:
                arabic_root = buckwalter_to_arabic_spaced(sx_root)
                break
        else:
            continue  # No root found, skip

        tx.run("""
        MATCH (ci) WHERE elementId(ci) = $eid
        SET ci.root = $root
        """, eid=element_id, root=arabic_root)

        console.log(f"[green]✔ Set root on node {element_id}: {arabic_root}")
        count += 1

    return count

def main():
    console.log("[blue]Starting Buckwalter → Arabic root conversion (processing all nodes)...")

    total = 0
    batch_count = 0
    
    try:
        while True:
            with driver.session() as session:
                updated = session.write_transaction(update_roots)
                if updated == 0:
                    break
                
                total += updated
                batch_count += 1
                
                console.log(f"[cyan]Batch {batch_count}: Updated {updated} nodes (Total: {total})")
                
                # Throttle to be gentle on Neo4j Aura
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