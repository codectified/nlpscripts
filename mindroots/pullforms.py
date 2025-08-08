import os
import csv
import time
from neo4j import GraphDatabase
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import track

# === Load environment ===
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")

# === Config ===
BATCH_SIZE = 5000
THROTTLE_SECONDS = 0.3
OUTPUT_FILE = "word_nodes_wazn_export2.csv"

# === Neo4j driver ===
driver = GraphDatabase.driver(uri, auth=(user, password))

# === Rich logging ===
console = Console()

# === Fetch batch ===
def fetch_batch(session, skip, limit):
    query = """
    MATCH (w:Word)
    OPTIONAL MATCH (r:Root)-[:HAS_WORD]->(w)
    RETURN 
        w.wazn AS form,
        w.arabic AS word_arabic,
        w.english AS gloss,
        w.entry_id AS id,
        w.itype AS itype,
        r.arabic AS root_arabic
    SKIP $skip LIMIT $limit
    """
    result = session.run(query, skip=skip, limit=limit)
    return [record.values() for record in result]

# === CSV Export ===
def export_all_words():
    with driver.session() as session, open(OUTPUT_FILE, mode="w", newline='', encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["form", "word_arabic", "gloss", "id", "itype", "root_arabic"])        
        skip = 0
        total_rows = 0

        console.rule("[bold cyan]🔄 Starting Export")
        while True:
            console.log(f"Fetching rows [yellow]{skip}[/yellow] to [yellow]{skip + BATCH_SIZE - 1}[/yellow]...")
            rows = fetch_batch(session, skip, BATCH_SIZE)

            if not rows:
                break

            writer.writerows(rows)
            total_rows += len(rows)
            skip += BATCH_SIZE
            console.log(f"[green]✓ Wrote {len(rows)} rows.[/green]")
            time.sleep(THROTTLE_SECONDS)

        console.rule("[bold green]✅ Export Complete")
        console.log(f"[bold]Total rows exported:[/bold] {total_rows} → [blue]{OUTPUT_FILE}[/blue]")

if __name__ == "__main__":
    export_all_words()
    driver.close()