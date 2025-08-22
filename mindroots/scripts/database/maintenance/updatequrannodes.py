import os
import time
from neo4j import GraphDatabase
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TimeElapsedColumn
from more_itertools import chunked

# === Load environment variables ===
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")

# === Console logging ===
console = Console()

# === Neo4j driver ===
driver = GraphDatabase.driver(uri, auth=(user, password))

# === Config ===
BATCH_SIZE = 500
THROTTLE_DELAY = 0.01  # seconds
TARGET_CORPUS_ID = 2


def fetch_items(session):
    query = """
    MATCH (ci:CorpusItem {corpus_id: $corpus_id})
    RETURN ci.item_id AS item_id, ci.sura_index AS sura, ci.aya_index AS aya
    ORDER BY ci.item_id
    """
    result = session.run(query, corpus_id=TARGET_CORPUS_ID)
    items = result.data()
    console.log(f"[green]Fetched {len(items)} CorpusItem nodes.")
    return items


def compute_positions(items):
    grouped = {}
    for item in items:
        key = (item["sura"], item["aya"])
        grouped.setdefault(key, []).append(item["item_id"])

    updates = []
    for (sura, aya), item_ids in grouped.items():
        for position, item_id in enumerate(item_ids, start=1):
            updates.append({"item_id": item_id, "position": position})

    console.log(f"[blue]Prepared {len(updates)} updates with computed positions.")
    return updates


def apply_batch(tx, batch):
    query = """
    UNWIND $batch AS row
    MATCH (ci:CorpusItem {item_id: row.item_id})
    SET ci.word_position = row.position
    RETURN row.item_id AS item_id
    """
    result = tx.run(query, batch=batch)
    return [record["item_id"] for record in result]


def update_all_positions():
    with driver.session() as session:
        console.log("[bold green]Starting update process...")
        items = fetch_items(session)
        updates = compute_positions(items)
        total = len(updates)

        failed = []

        with Progress(
            SpinnerColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[yellow]Updating word_position...", total=total)

            for batch in chunked(updates, BATCH_SIZE):
                try:
                    updated = session.write_transaction(apply_batch, batch)
                    if len(updated) < len(batch):
                        failed_ids = [row["item_id"] for row in batch if row["item_id"] not in updated]
                        failed.extend(failed_ids)
                except Exception as e:
                    console.log(f"[red]Batch failed: {e}")
                    failed.extend([row["item_id"] for row in batch])

                progress.advance(task, len(batch))
                time.sleep(THROTTLE_DELAY)

        if failed:
            console.log(f"[bold red]⚠️ {len(failed)} items failed to update.")
        else:
            console.log(f"[bold green]✅ All word_position values updated successfully.")


if __name__ == "__main__":
    try:
        update_all_positions()
    finally:
        driver.close()
        console.log("[bold red]Neo4j connection closed.")