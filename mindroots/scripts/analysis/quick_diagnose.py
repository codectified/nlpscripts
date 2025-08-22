#!/usr/bin/env python3

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USER') 
password = os.getenv('NEO4J_PASS')

driver = GraphDatabase.driver(uri, auth=(user, password))

def quick_check():
    with driver.session() as session:
        print("=== QUICK DIAGNOSIS ===\n")
        
        # 1. Key counts
        print("1. KEY COUNTS:")
        result = session.run("MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 RETURN count(ci) as total")
        total = result.single()['total']
        print(f"   Total corpus items: {total}")
        
        result = session.run("MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND (ci)-[:HAS_WORD]->(:Word) RETURN count(ci) as linked")
        linked = result.single()['linked']
        print(f"   Already linked: {linked}")
        print(f"   Unlinked: {total - linked}")
        
        # 2. Root property status
        print("\n2. ROOT PROPERTY STATUS:")
        result = session.run("MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND ci.root IS NOT NULL RETURN count(ci) as with_root")
        with_root = result.single()['with_root']
        print(f"   Items with root property: {with_root}")
        
        result = session.run("MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND ci.root IS NOT NULL AND ci.root <> 'None' RETURN count(ci) as valid_root")
        valid_root = result.single()['valid_root']
        print(f"   Items with valid (non-'None') root: {valid_root}")
        
        # 3. Check if n_root exists (should be 0 based on previous output)
        result = session.run("MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND ci.n_root IS NOT NULL RETURN count(ci) as with_n_root")
        with_n_root = result.single()['with_n_root']
        print(f"   Items with n_root property: {with_n_root}")
        
        # 4. Items ready to process
        print("\n3. PROCESSABLE ITEMS:")
        result = session.run("MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND ci.root IS NOT NULL AND ci.root <> 'None' AND NOT (ci)-[:HAS_WORD]->(:Word) RETURN count(ci) as processable")
        processable = result.single()['processable']
        print(f"   Unlinked items with valid roots: {processable}")
        
        # 5. Sample processable items
        print("\n4. SAMPLE PROCESSABLE ITEMS:")
        result = session.run("""
            MATCH (ci:CorpusItem) 
            WHERE ci.corpus_id = 2 AND ci.root IS NOT NULL AND ci.root <> 'None' 
              AND NOT (ci)-[:HAS_WORD]->(:Word)
            RETURN ci.item_id, ci.lemma, ci.root
            LIMIT 3
        """)
        
        for record in result:
            print(f"   item_id: {record['ci.item_id']}, lemma: '{record['ci.lemma']}', root: '{record['ci.root']}'")

if __name__ == "__main__":
    quick_check()
    driver.close()