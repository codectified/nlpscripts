"""
Detailed XML Analysis of Richest Lane's Entries
==============================================

Now that we've established NodeID mapping works, let's do comprehensive
analysis of the largest, richest entries to find extraction patterns.
"""

import os
import sqlite3
import re
from xml.etree import ElementTree as ET
from xml.dom import minidom
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()
neo4j_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
)

def get_largest_entries(tx, limit=5):
    """Get largest Neo4j Word entries"""
    query = """
    MATCH (w:Word)
    WHERE w.dataSize IS NOT NULL AND w.entry_id IS NOT NULL
    RETURN DISTINCT w.arabic, w.entry_id, w.dataSize
    ORDER BY w.dataSize DESC
    LIMIT $limit
    """
    result = tx.run(query, limit=limit)
    return [(record["w.arabic"], record["w.entry_id"], record["w.dataSize"]) for record in result]

def get_xml_by_nodeid(conn, nodeid):
    """Get SQL entry XML by nodeid"""
    cursor = conn.cursor()
    cursor.execute("SELECT id, word, root, xml FROM entry WHERE nodeid = ?", (nodeid,))
    return cursor.fetchone()

def pretty_print_xml_formatted(xml_string, max_lines=100):
    """Pretty print XML with better formatting"""
    try:
        root = ET.fromstring(xml_string)
        rough_string = ET.tostring(root, encoding='unicode')
        reparsed = minidom.parseString(rough_string)
        pretty = reparsed.toprettyxml(indent="  ")

        lines = [line for line in pretty.split('\\n') if line.strip()]

        if len(lines) > max_lines:
            displayed_lines = lines[1:max_lines]  # Skip XML declaration
            displayed_lines.append(f"... [TRUNCATED - showing {max_lines-1} of {len(lines)-1} lines]")
            return '\\n'.join(displayed_lines)
        else:
            return '\\n'.join(lines[1:])  # Skip XML declaration

    except ET.ParseError as e:
        return f"XML Parse Error: {e}"

def extract_sense_hierarchy(xml_string):
    """Extract detailed sense hierarchy from XML"""
    try:
        root = ET.fromstring(xml_string)

        hierarchy = {
            'primary_senses': [],
            'secondary_senses': [],
            'alternative_senses': [],
            'special_senses': []
        }

        # Find all sense elements
        for sense in root.findall('.//sense'):
            sense_data = {
                'type': sense.get('type', 'standard'),
                'n': sense.get('n', ''),
                'text': sense.text.strip() if sense.text else '',
                'tail': sense.tail.strip() if sense.tail else ''
            }

            # Classify sense by type and number
            sense_type = sense_data['type']
            sense_n = sense_data['n']

            if sense_type == 'b':
                hierarchy['secondary_senses'].append(sense_data)
            elif sense_type == 'A':
                hierarchy['alternative_senses'].append(sense_data)
            elif sense_type in ['tropical', 'contrary']:
                hierarchy['special_senses'].append(sense_data)
            else:
                hierarchy['primary_senses'].append(sense_data)

        return hierarchy

    except ET.ParseError:
        return None

def extract_references_and_citations(xml_string):
    """Extract all types of references and citations"""
    try:
        root = ET.fromstring(xml_string)

        extracts = {
            'cross_references': [],
            'quranic_citations': [],
            'poetry_citations': [],
            'proverb_markers': [],
            'foreign_text': []
        }

        # XML-based references
        for ref in root.findall('.//ref'):
            extracts['cross_references'].append({
                'target': ref.get('target'),
                'type': ref.get('type'),
                'cref': ref.get('cref'),
                'text': ref.text
            })

        # Foreign language text (Arabic)
        for foreign in root.findall('.//foreign'):
            if foreign.get('lang') == 'ar':
                extracts['foreign_text'].append({
                    'text': foreign.text,
                    'context': 'arabic_text'
                })

        # Text-based pattern extraction
        full_text = ET.tostring(root, encoding='unicode', method='text')

        # Quranic citations
        qur_patterns = [
            r'\\bin the Kur[^.]*',
            r'\\bKur[^.]*ch\\.[^.]*',
            r'\\[in the Kur[^\\]]*\\]'
        ]

        for pattern in qur_patterns:
            matches = re.finditer(pattern, full_text, re.IGNORECASE)
            for match in matches:
                extracts['quranic_citations'].append({
                    'text': match.group(0),
                    'pattern': pattern,
                    'start': match.start(),
                    'end': match.end()
                })

        # Poetry patterns
        poetry_patterns = [
            r'verse of [A-Z][^.,]{3,30}',
            r'poet[^.]{0,50}says?[^.]*',
            r'poetry[^.]{0,100}'
        ]

        for pattern in poetry_patterns:
            matches = re.finditer(pattern, full_text, re.IGNORECASE)
            for match in matches:
                extracts['poetry_citations'].append({
                    'text': match.group(0),
                    'pattern': pattern
                })

        # Proverb patterns
        proverb_patterns = [
            r'\\b[Ii]t is a proverb[^.]*',
            r'\\bproverb[^.]{0,100}',
            r'\\bsaying[^.]{0,100}'
        ]

        for pattern in proverb_patterns:
            matches = re.finditer(pattern, full_text, re.IGNORECASE)
            for match in matches:
                extracts['proverb_markers'].append({
                    'text': match.group(0),
                    'pattern': pattern
                })

        return extracts

    except ET.ParseError:
        return None

def analyze_entry_structure(xml_string):
    """Analyze overall XML entry structure"""
    try:
        root = ET.fromstring(xml_string)

        structure = {
            'root_tag': root.tag,
            'attributes': dict(root.attrib),
            'element_counts': {},
            'total_text_length': len(ET.tostring(root, encoding='unicode', method='text')),
            'nesting_depth': 0
        }

        # Count all element types
        def count_elements(elem, depth=0):
            structure['nesting_depth'] = max(structure['nesting_depth'], depth)

            tag = elem.tag
            structure['element_counts'][tag] = structure['element_counts'].get(tag, 0) + 1

            for child in elem:
                count_elements(child, depth + 1)

        count_elements(root)

        return structure

    except ET.ParseError:
        return None

def main():
    print("📖 DETAILED XML ANALYSIS OF RICHEST LANE'S ENTRIES")
    print("=" * 70)

    sql_conn = sqlite3.connect("/Users/omaribrahim/Downloads/LexiconDatabase-1.0.9/lexicon.sqlite")

    try:
        with neo4j_driver.session() as session:
            largest_entries = session.execute_read(get_largest_entries, 3)

            for i, (arabic, entry_id, data_size) in enumerate(largest_entries, 1):
                print(f"\\n{'='*20} ENTRY {i}: {arabic} {'='*20}")
                print(f"Entry ID: {entry_id} | Data Size: {data_size:,}")

                # Get XML from SQL
                sql_result = get_xml_by_nodeid(sql_conn, entry_id)
                if not sql_result:
                    print("❌ No SQL match found")
                    continue

                sql_id, word, root, xml = sql_result
                print(f"SQL ID: {sql_id} | Word: {word} | Root: {root}")
                print(f"XML Length: {len(xml):,} characters\\n")

                # 1. Overall structure analysis
                print("🏗️ STRUCTURE ANALYSIS:")
                print("-" * 30)
                structure = analyze_entry_structure(xml)
                if structure:
                    print(f"Root tag: {structure['root_tag']}")
                    print(f"Attributes: {structure['attributes']}")
                    print(f"Text length: {structure['total_text_length']:,} chars")
                    print(f"Nesting depth: {structure['nesting_depth']}")
                    print(f"Element counts: {dict(sorted(structure['element_counts'].items(), key=lambda x: x[1], reverse=True)[:10])}")

                # 2. Sense hierarchy analysis
                print("\\n🎯 SENSE HIERARCHY:")
                print("-" * 30)
                hierarchy = extract_sense_hierarchy(xml)
                if hierarchy:
                    for sense_type, senses in hierarchy.items():
                        if senses:
                            print(f"{sense_type}: {len(senses)} senses")
                            for j, sense in enumerate(senses[:3]):
                                marker = sense['text'] if sense['text'] else f"type={sense['type']}, n={sense['n']}"
                                print(f"  {j+1}. {marker}")
                                if sense['tail']:
                                    tail_preview = sense['tail'][:100] + "..." if len(sense['tail']) > 100 else sense['tail']
                                    print(f"     Content: {tail_preview}")

                # 3. References and citations
                print("\\n📚 REFERENCES & CITATIONS:")
                print("-" * 30)
                extracts = extract_references_and_citations(xml)
                if extracts:
                    for extract_type, items in extracts.items():
                        if items:
                            print(f"{extract_type}: {len(items)} found")
                            for j, item in enumerate(items[:2]):
                                if isinstance(item, dict):
                                    if 'text' in item and item['text']:
                                        text = item['text'][:100] + "..." if len(item['text']) > 100 else item['text']
                                        print(f"  {j+1}. {text}")
                                    elif 'target' in item:
                                        print(f"  {j+1}. Target: {item['target']}, Type: {item['type']}")

                # 4. Pretty-printed XML sample
                print("\\n📋 XML STRUCTURE SAMPLE:")
                print("-" * 30)
                pretty_xml = pretty_print_xml_formatted(xml, 40)
                print(pretty_xml)

                print("\\n" + "="*70)

            print("\\n🎯 KEY FINDINGS & EXTRACTION OPPORTUNITIES:")
            print("="*50)
            print("1. ✅ Sense hierarchy is well-structured with <sense> elements")
            print("2. ✅ References use <ref> elements with target/type attributes")
            print("3. ✅ Arabic text is in <foreign lang='ar'> elements")
            print("4. ✅ Sense markers (-b2-, -A2-) are in sense text content")
            print("5. ✅ Citations follow text patterns (Kur, verse of, proverb)")
            print("\\n🚀 NEXT: Design extraction pipeline based on these patterns!")

    finally:
        sql_conn.close()
        neo4j_driver.close()

if __name__ == "__main__":
    main()