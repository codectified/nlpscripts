"""
Analyze ID Mapping Between Neo4j and SQL Database
===============================================

Figure out how Neo4j entry_id relates to SQL database ID field.
Neo4j has format 'n31285' while SQL has numeric IDs.
"""

import os
import sqlite3
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()
neo4j_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
)

def analyze_neo4j_entry_ids(tx):
    """Analyze Neo4j entry_id patterns"""
    query = """
    MATCH (w:Word)
    WHERE w.entry_id IS NOT NULL
    RETURN w.arabic, w.entry_id, w.dataSize
    ORDER BY w.dataSize DESC
    LIMIT 10
    """
    result = tx.run(query)
    return [(record["w.arabic"], record["w.entry_id"], record["w.dataSize"]) for record in result]

def analyze_sql_ids(conn):
    """Analyze SQL database ID patterns"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, word, root, nodeid, LENGTH(xml) as xml_length
        FROM entry
        ORDER BY LENGTH(xml) DESC
        LIMIT 10
    """)
    return cursor.fetchall()

def search_by_word_name(conn, arabic_word):
    """Try to find SQL entry by Arabic word name"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, word, bword, root, nodeid, LENGTH(xml) as xml_length
        FROM entry
        WHERE word = ? OR bword = ?
        ORDER BY LENGTH(xml) DESC
        LIMIT 5
    """, (arabic_word, arabic_word))
    return cursor.fetchall()

def extract_numeric_from_entry_id(entry_id):
    """Extract numeric part from Neo4j entry_id (e.g., 'n31285' → 31285)"""
    import re
    match = re.search(r'n?(\d+)', entry_id)
    return int(match.group(1)) if match else None

def main():
    print("🔍 ANALYZING ID MAPPING: Neo4j ↔ SQL Database")
    print("=" * 60)

    sql_conn = sqlite3.connect("/Users/omaribrahim/Downloads/LexiconDatabase-1.0.9/lexicon.sqlite")

    try:
        print("\\n📊 Neo4j Entry ID Patterns:")
        print("-" * 40)

        with neo4j_driver.session() as session:
            neo4j_entries = session.execute_read(analyze_neo4j_entry_ids)

            for i, (arabic, entry_id, data_size) in enumerate(neo4j_entries, 1):
                numeric_id = extract_numeric_from_entry_id(entry_id)
                print(f"{i}. {arabic} | Entry ID: {entry_id} | Numeric: {numeric_id} | Size: {data_size}")

        print("\\n📊 SQL Database ID Patterns:")
        print("-" * 40)

        sql_entries = analyze_sql_ids(sql_conn)
        for i, (sql_id, word, root, nodeid, xml_length) in enumerate(sql_entries, 1):
            print(f"{i}. ID: {sql_id} | Word: {word} | NodeID: {nodeid} | XML Length: {xml_length}")

        print("\\n🔗 Attempting to Match by Numeric ID:")
        print("-" * 40)

        # Try to match largest Neo4j entries to SQL by numeric ID
        for arabic, entry_id, data_size in neo4j_entries[:3]:
            numeric_id = extract_numeric_from_entry_id(entry_id)
            print(f"\\n🔍 Neo4j: {arabic} (ID: {entry_id} → {numeric_id})")

            if numeric_id:
                cursor = sql_conn.cursor()
                cursor.execute("SELECT id, word, root, xml FROM entry WHERE id = ?", (numeric_id,))
                sql_match = cursor.fetchone()

                if sql_match:
                    sql_id, sql_word, sql_root, xml = sql_match
                    print(f"✅ SQL Match: ID {sql_id} | Word: {sql_word} | Root: {sql_root}")
                    print(f"   XML Length: {len(xml):,} characters")

                    # Show XML sample
                    xml_sample = xml[:500] + "..." if len(xml) > 500 else xml
                    print(f"   XML Sample: {xml_sample}")
                else:
                    print("❌ No SQL match for numeric ID")

                    # Try word name matching as fallback
                    print(f"🔍 Trying word name match for '{arabic}'...")
                    word_matches = search_by_word_name(sql_conn, arabic)

                    if word_matches:
                        print("✅ Found by word name:")
                        for match in word_matches[:2]:
                            sql_id, word, bword, root, nodeid, xml_length = match
                            print(f"   ID: {sql_id} | Word: {word} | BWord: {bword} | Root: {root}")
                    else:
                        print("❌ No word name matches")

        print("\\n🔗 Testing NodeID Matching:")
        print("-" * 40)

        # Check if Neo4j entry_id matches SQL nodeid field
        for arabic, entry_id, data_size in neo4j_entries[:3]:
            cursor = sql_conn.cursor()
            cursor.execute("SELECT id, word, root, nodeid, xml FROM entry WHERE nodeid = ?", (entry_id,))
            nodeid_match = cursor.fetchone()

            if nodeid_match:
                sql_id, sql_word, sql_root, nodeid, xml = nodeid_match
                print(f"✅ NodeID Match: {entry_id}")
                print(f"   SQL ID: {sql_id} | Word: {sql_word} | Root: {sql_root}")
                print(f"   XML Length: {len(xml):,} characters")

                # This is our winner! Show XML structure
                print(f"\\n📋 XML Structure Preview:")
                try:
                    from xml.etree import ElementTree as ET
                    root = ET.fromstring(xml)
                    print(f"   Root tag: {root.tag}")
                    print(f"   Attributes: {root.attrib}")

                    # Count sense elements
                    senses = root.findall('.//sense')
                    print(f"   Sense elements: {len(senses)}")

                    # Count foreign elements (Arabic text)
                    foreign = root.findall('.//foreign')
                    print(f"   Foreign elements: {len(foreign)}")

                    # Count references
                    refs = root.findall('.//ref')
                    print(f"   References: {len(refs)}")

                except ET.ParseError:
                    print("   (XML parse error)")

            else:
                print(f"❌ No NodeID match for {entry_id}")

    finally:
        sql_conn.close()
        neo4j_driver.close()

if __name__ == "__main__":
    main()