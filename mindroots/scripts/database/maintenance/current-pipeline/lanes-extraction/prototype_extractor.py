"""
Prototype SQL → Neo4j Extraction Pipeline
=======================================

Direct implementation of Option 1: Single Definition Node approach.
Extracts XML from SQL database and creates Definition nodes in Neo4j
with extracted properties for sense hierarchy and semantic data.
"""

import os
import sqlite3
import re
from xml.etree import ElementTree as ET
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()
neo4j_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
)

def extract_xml_properties(xml_string):
    """Extract all key properties from XML for Definition node"""
    import json

    try:
        root = ET.fromstring(xml_string)

        properties = {
            'xml_source': xml_string,
            'xml_length': len(xml_string),
            'entry_tag': root.tag,
            'entry_attributes': json.dumps(dict(root.attrib))
        }

        # Extract sense hierarchy
        senses = []
        sense_elements = root.findall('.//sense')

        for sense_elem in sense_elements:
            sense_data = {
                'sense_id': f"{sense_elem.get('type', 'primary')}{sense_elem.get('n', '')}",
                'sense_type': sense_elem.get('type', 'standard'),
                'sense_number': sense_elem.get('n'),
                'marker_text': sense_elem.text.strip() if sense_elem.text else '',
                'definition_text': sense_elem.tail.strip() if sense_elem.tail else '',
                'is_primary': sense_elem.get('type') not in ['b', 'A'],
                'is_secondary': sense_elem.get('type') == 'b',
                'is_alternative': sense_elem.get('type') == 'A'
            }
            senses.append(sense_data)

        # Store senses as JSON string for Neo4j compatibility
        properties['extracted_senses'] = json.dumps(senses)
        properties['sense_count'] = len(senses)

        # Extract cross-references
        references = []
        ref_elements = root.findall('.//ref')

        for ref_elem in ref_elements:
            ref_data = {
                'target_word': ref_elem.get('target'),
                'reference_type': ref_elem.get('type'),
                'cross_ref_id': ref_elem.get('cref'),
                'context': ref_elem.text
            }
            references.append(ref_data)

        # Store references as JSON string
        properties['extracted_references'] = json.dumps(references)
        properties['reference_count'] = len(references)

        # Extract Arabic text
        arabic_texts = []
        foreign_elements = root.findall('.//foreign[@lang="ar"]')

        for foreign_elem in foreign_elements:
            if foreign_elem.text:
                arabic_texts.append({
                    'text': foreign_elem.text,
                    'context': 'definition',
                    'normalized': normalize_arabic(foreign_elem.text)
                })

        # Store Arabic texts as JSON string
        properties['extracted_arabic'] = json.dumps(arabic_texts)
        properties['arabic_text_count'] = len(arabic_texts)

        # Detect signification types
        full_text = ET.tostring(root, encoding='unicode', method='text')

        has_tropical = bool(re.search(r'tropical|assumedtropical', full_text, re.IGNORECASE))
        has_contrary = bool(re.search(r'contrary', full_text, re.IGNORECASE))

        properties['has_tropical_senses'] = has_tropical
        properties['has_contrary_senses'] = has_contrary

        # Extract primary definition (first substantial text)
        primary_def = extract_primary_definition(full_text)
        properties['primary_definition'] = primary_def[:500] if primary_def else ""

        # Element counts for analysis
        element_counts = {}
        for elem in root.iter():
            tag = elem.tag
            element_counts[tag] = element_counts.get(tag, 0) + 1

        # Store element counts as JSON string
        properties['element_counts'] = json.dumps(element_counts)

        return properties

    except ET.ParseError as e:
        return {'error': f"XML Parse Error: {e}", 'xml_source': xml_string}

def normalize_arabic(text):
    """Basic Arabic text normalization"""
    if not text:
        return ""

    import unicodedata

    # NFKD normalization
    normalized = unicodedata.normalize('NFKD', text)

    # Remove diacritics
    diacritic_pattern = r'[\u064B-\u0655\u0670]'
    normalized = re.sub(diacritic_pattern, '', normalized)

    # Normalize alifs
    alif_pattern = r'[أإآٱ]'
    normalized = re.sub(alif_pattern, 'ا', normalized)

    # Normalize ya
    normalized = normalized.replace('ى', 'ي')

    return normalized.strip()

def extract_primary_definition(full_text):
    """Extract the main definition text from XML text content"""
    # Remove XML tags and get plain text
    clean_text = re.sub(r'<[^>]+>', ' ', full_text)

    # Look for substantial definition content (not just markers)
    sentences = re.split(r'[.!?]+', clean_text)

    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) > 50 and not sentence.startswith('-'):
            return sentence

    return clean_text[:200].strip() if clean_text else ""

def get_target_entries(tx, limit=10):
    """Get Word nodes with entry_id for processing"""
    query = """
    MATCH (w:Word)
    WHERE w.entry_id IS NOT NULL
    AND w.dataSize IS NOT NULL
    AND NOT EXISTS((w)-[:HAS_DEFINITION]->())
    RETURN w.arabic, w.entry_id, w.dataSize
    ORDER BY w.dataSize DESC
    LIMIT $limit
    """
    result = tx.run(query, limit=limit)
    return [(record["w.arabic"], record["w.entry_id"], record["w.dataSize"])
            for record in result]

def get_xml_by_nodeid(conn, nodeid):
    """Get XML from SQL database by nodeid"""
    cursor = conn.cursor()
    cursor.execute("SELECT id, word, root, xml FROM entry WHERE nodeid = ?", (nodeid,))
    return cursor.fetchone()

def create_definition_node(tx, word_entry_id, properties):
    """Create Definition node and link to Word"""
    query = """
    MATCH (w:Word {entry_id: $entry_id})
    CREATE (w)-[:HAS_DEFINITION]->(d:Definition)
    SET d += $properties
    RETURN d.entry_id as created_entry_id
    """
    result = tx.run(query, entry_id=word_entry_id, properties=properties)
    return result.single()

def main():
    print("🚀 PROTOTYPE SQL → NEO4J EXTRACTION PIPELINE")
    print("=" * 60)
    print("Implementing Option 1: Single Definition Node approach\n")

    sql_conn = sqlite3.connect("/Users/omaribrahim/Downloads/LexiconDatabase-1.0.9/lexicon.sqlite")

    try:
        with neo4j_driver.session() as session:
            # Get target entries for processing
            print("📋 Selecting target entries for extraction...")
            target_entries = session.execute_read(get_target_entries, 5)

            print(f"Found {len(target_entries)} entries to process:\n")
            for i, (arabic, entry_id, data_size) in enumerate(target_entries, 1):
                print(f"{i}. {arabic} | Entry ID: {entry_id} | Size: {data_size:,}")

            print("\n" + "=" * 60)
            print("🔄 Processing entries...")

            for i, (arabic, entry_id, data_size) in enumerate(target_entries, 1):
                print(f"\n📖 Processing {i}/{len(target_entries)}: {arabic} ({entry_id})")
                print("-" * 40)

                # Get XML from SQL
                sql_result = get_xml_by_nodeid(sql_conn, entry_id)
                if not sql_result:
                    print(f"❌ No SQL match found for {entry_id}")
                    continue

                sql_id, word, root, xml = sql_result
                print(f"✅ Found SQL entry: {word} (Root: {root})")
                print(f"   XML length: {len(xml):,} characters")

                # Extract properties from XML
                print("🔍 Extracting XML properties...")
                properties = extract_xml_properties(xml)

                if 'error' in properties:
                    print(f"❌ Extraction error: {properties['error']}")
                    continue

                # Add entry metadata
                properties.update({
                    'entry_id': entry_id,
                    'sql_word': word,
                    'sql_root': root,
                    'sql_id': sql_id,
                    'neo4j_data_size': data_size
                })

                print(f"📊 Extracted properties:")
                print(f"   - Senses: {properties['sense_count']}")
                print(f"   - References: {properties['reference_count']}")
                print(f"   - Arabic texts: {properties['arabic_text_count']}")
                print(f"   - Has tropical: {properties['has_tropical_senses']}")
                print(f"   - Has contrary: {properties['has_contrary_senses']}")
                print(f"   - Primary def: {properties['primary_definition'][:100]}...")

                # Create Definition node in Neo4j
                print("💾 Creating Definition node...")
                try:
                    result = session.execute_write(create_definition_node, entry_id, properties)
                    if result:
                        print(f"✅ Created Definition node for {entry_id}")
                    else:
                        print(f"❌ Failed to create Definition node for {entry_id}")
                except Exception as e:
                    print(f"❌ Neo4j error: {e}")

                print("-" * 40)

            print(f"\n🎯 EXTRACTION COMPLETE!")
            print("=" * 60)

            # Verify results
            print("\n📊 Verification query:")
            verify_query = """
            MATCH (w:Word)-[:HAS_DEFINITION]->(d:Definition)
            RETURN count(*) as total_definitions,
                   avg(d.sense_count) as avg_senses,
                   sum(d.reference_count) as total_references,
                   sum(d.arabic_text_count) as total_arabic_texts
            """

            result = session.run(verify_query).single()
            if result:
                total_defs = result['total_definitions'] or 0
                avg_senses = result['avg_senses'] or 0
                total_refs = result['total_references'] or 0
                total_arabic = result['total_arabic_texts'] or 0

                print(f"✅ Created {total_defs} Definition nodes")
                print(f"   Average senses per entry: {avg_senses:.1f}")
                print(f"   Total references extracted: {total_refs}")
                print(f"   Total Arabic texts: {total_arabic}")

            print("\n🚀 NEXT STEPS:")
            print("1. Run on larger batch of entries")
            print("2. Create specialized Sense nodes if needed")
            print("3. Implement full pipeline automation")

    finally:
        sql_conn.close()
        neo4j_driver.close()

if __name__ == "__main__":
    main()