import os
import time
from neo4j import GraphDatabase
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import track

# === Load environment variables ===
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")

# === Logging setup ===
console = Console()

# === itype → wazn mapping ===
itype_to_wazn = {
    "1": "فَعَلَ",
    "2": "فَاعَلَ",
    "3": "فَعَّلَ",
    "4": "أَفْعَلَ",
    "5": "تَفَعَّلَ",
    "6": "تَفَاعَلَ",
    "7": "انْفَعَلَ",
    "8": "افْتَعَلَ",
    "9": "افْعَلَّ",
    "10": "اسْتَفْعَلَ",
    "11": "افْعَالَّ",
    "12": "افْعَوَلَ",
    "13": "افْعَوَّلَ",
    "Q. 1": "فَعْلَلَ",
    "Q.1": "فَعْلَلَ",
    "Q. 2": "تَفَعْلَلَ",
    "Q.2": "تَفَعْلَلَ",
    "Q. 3": "افْعَنْلَلَ",
    "Q. 4": "افْعَلَلَّ",
    "Q. Q. 1": "فَعْلَلَ",
    "Q. Q. 2": "تَفَعْلَلَ",
    "Q. Q. 3": "افْعَنْلَلَ",
    "Q. Q. 4": "افْعَلَلَّ",
    "Q. Q.2": "تَفَعْلَلَ",
    "R. Q. 1": "فَعْلَلَ",
    "R. Q. 2": "تَفَعْلَلَ",
    "R. Q. 3": "افْعَنْلَلَ",
    "R. Q. 4": "افْعَلَلَّ",
    "R. Q.1": "فَعْلَلَ",
    "R.Q.1": "فَعْلَلَ"
}

# === Cypher updater ===
def update_wazn_for_itype(tx, itype, wazn):
    result = tx.run("""
        MATCH (w:Word)
        WHERE w.itype = $itype
        SET w.wazn = $wazn
        RETURN count(w) AS updated
    """, itype=itype, wazn=wazn)
    return result.single()["updated"]

# === Main logic ===
def main():
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        console.rule("[bold green]Wazn Property Update Started")
        for itype in track(itype_to_wazn, description="Updating wazn..."):
            wazn = itype_to_wazn[itype]
            try:
                count = session.write_transaction(update_wazn_for_itype, itype, wazn)
                console.log(f"[green]✓[/green] Set [bold]{wazn}[/bold] for [cyan]{itype}[/cyan] → [magenta]{count}[/magenta] nodes updated.")
            except Exception as e:
                console.log(f"[red]✗ Error updating itype {itype}[/red]: {e}")
            time.sleep(0.3)  # Throttle for Aura
        console.rule("[bold green]Update Complete ✅")
    driver.close()

if __name__ == "__main__":
    main()