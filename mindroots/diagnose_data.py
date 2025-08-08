#!/usr/bin/env python3

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import json

load_dotenv()

uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USER') 
password = os.getenv('NEO4J_PASS')

driver = GraphDatabase.driver(uri, auth=(user, password))

def explore_database():
    with driver.session() as session:
        print("=== COMPREHENSIVE DATABASE EXPLORATION ===\n")
        
        # 1. Basic counts
        print("1. BASIC COUNTS:")
        result = session.run("MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 RETURN count(ci) as total")
        total = result.single()['total']
        print(f"   Total CorpusItem nodes with corpus_id=2: {total}")
        
        # 2. Property exploration - what properties actually exist?
        print("\n2. PROPERTY ANALYSIS:")
        result = session.run("""
            MATCH (ci:CorpusItem) 
            WHERE ci.corpus_id = 2
            WITH ci, keys(ci) as props
            UNWIND props as prop
            RETURN DISTINCT prop, count(*) as count
            ORDER BY prop
        """)
        print("   Properties found on CorpusItem nodes:")
        for record in result:
            print(f"     {record['prop']}: {record['count']} nodes")
        
        # 3. Check specific root properties
        print("\n3. ROOT PROPERTY CHECKS:")
        
        # n_root checks
        result = session.run("MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND ci.n_root IS NOT NULL RETURN count(ci) as with_n_root")
        with_n_root = result.single()['with_n_root']
        print(f"   CorpusItems with n_root IS NOT NULL: {with_n_root}")
        
        # Since n_root doesn't exist, skip the n_root property check
        print(f"   CorpusItems with n_root property existing: 0 (property doesn't exist)")
        
        # root checks  
        result = session.run("MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND ci.root IS NOT NULL RETURN count(ci) as with_root")
        with_root = result.single()['with_root']
        print(f"   CorpusItems with root IS NOT NULL: {with_root}")
        
        # 4. Sample data with all properties
        print("\n4. SAMPLE DATA (first 10 items):")
        result = session.run("""
            MATCH (ci:CorpusItem) 
            WHERE ci.corpus_id = 2
            RETURN ci.item_id, ci.lemma, ci.root, properties(ci) as all_props
            LIMIT 10
        """)
        for i, record in enumerate(result, 1):
            print(f"   Item {i}:")
            print(f"     item_id: {record['ci.item_id']}")
            print(f"     lemma: '{record['ci.lemma']}'")  
            print(f"     root: '{record['ci.root']}'")
            print(f"     all_props: {json.dumps(dict(record['all_props']), ensure_ascii=False, indent=8)}")
            print()
        
        # 5. Check for null vs empty vs missing patterns
        print("5. NULL/EMPTY/MISSING PATTERNS:")
        
        queries = [
            ("root = null (literal)", "MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND ci.root = null RETURN count(ci) as cnt"),
            ("root = 'None' (string)", "MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND ci.root = 'None' RETURN count(ci) as cnt"),
            ("root = '' (empty)", "MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND ci.root = '' RETURN count(ci) as cnt"),
            ("root IS NULL", "MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND ci.root IS NULL RETURN count(ci) as cnt")
        ]
        
        for desc, query in queries:
            result = session.run(query)
            count = result.single()['cnt']
            print(f"   {desc}: {count}")
        
        # 6. Check linking status
        print("\n6. LINKING STATUS:")
        result = session.run("MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND (ci)-[:HAS_WORD]->(:Word) RETURN count(ci) as linked")
        linked = result.single()['linked']
        print(f"   Already linked items: {linked}")
        print(f"   Unlinked items: {total - linked}")
        
        # 7. Look for items that should be processable
        print("\n7. POTENTIALLY PROCESSABLE ITEMS:")
        
        # Try different combinations to find what we can actually process
        test_queries = [
            ("Has root, not linked", "MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND ci.root IS NOT NULL AND NOT (ci)-[:HAS_WORD]->(:Word) RETURN count(ci) as cnt"),
            ("Has root != 'None', not linked", "MATCH (ci:CorpusItem) WHERE ci.corpus_id = 2 AND ci.root IS NOT NULL AND ci.root <> 'None' AND NOT (ci)-[:HAS_WORD]->(:Word) RETURN count(ci) as cnt")
        ]
        
        for desc, query in test_queries:
            result = session.run(query)
            count = result.single()['cnt']
            print(f"   {desc}: {count}")
        
        # 8. Sample of items with valid roots
        print("\n8. SAMPLE ITEMS WITH VALID ROOTS:")
        result = session.run("""
            MATCH (ci:CorpusItem) 
            WHERE ci.corpus_id = 2 AND ci.root IS NOT NULL AND ci.root <> 'None' 
              AND NOT (ci)-[:HAS_WORD]->(:Word)
            RETURN ci.item_id, ci.lemma, ci.root
            LIMIT 5
        """)
        
        for record in result:
            print(f"   item_id: {record['ci.item_id']}, lemma: '{record['ci.lemma']}', root: '{record['ci.root']}'")

if __name__ == "__main__":
    explore_database()
    driver.close()