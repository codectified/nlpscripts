"""
Trace Largest Word Entries: Neo4j → SQL → XML Analysis
=====================================================

Step-by-step documentation of finding the richest Lane's entries:
1. Query Neo4j for largest Word nodes by dataSize
2. Map entry_id to SQL database
3. Extract and pretty-print XML
4. Analyze sense hierarchy and references
5. Document extraction patterns

This will reveal the lowest-hanging fruit for sense/reference extraction.
"""

import os
import sqlite3
from xml.etree import ElementTree as ET
from xml.dom import minidom
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Setup connections
load_dotenv()
neo4j_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
)

def get_largest_word_nodes(tx, limit=10):
    """Step 1: Query Neo4j for largest Word nodes by dataSize"""
    query = """
    MATCH (w:Word)
    WHERE w.dataSize IS NOT NULL AND w.entry_id IS NOT NULL
    RETURN w.arabic, w.entry_id, w.dataSize, w.definitions
    ORDER BY w.dataSize DESC
    LIMIT $limit
    """
    result = tx.run(query, limit=limit)
    return [(record["w.arabic"], record["w.entry_id"], record["w.dataSize"], record["w.definitions"])
            for record in result]

def get_sql_entry_by_id(conn, entry_id):
    """Step 2: Map entry_id to SQL database entry"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, root, word, bword, xml, page, nodenum
        FROM entry
        WHERE id = ?
    """, (entry_id,))
    return cursor.fetchone()

def pretty_print_xml(xml_string):
    """Step 3: Pretty-print XML for analysis"""
    try:
        # Parse and reformat XML
        root = ET.fromstring(xml_string)
        rough_string = ET.tostring(root, encoding='unicode')
        reparsed = minidom.parseString(rough_string)
        pretty = reparsed.toprettyxml(indent="  ")

        # Remove empty lines and fix formatting
        lines = [line for line in pretty.split('\n') if line.strip()]
        return '\n'.join(lines[1:])  # Skip XML declaration

    except ET.ParseError as e:
        return f"XML Parse Error: {e}\n\nRaw XML:\n{xml_string[:1000]}..."

def analyze_sense_structure(xml_string):
    """Step 4: Analyze sense hierarchy in XML"""
    try:
        root = ET.fromstring(xml_string)

        analysis = {
            'total_senses': 0,
            'sense_types': {},
            'sense_markers': [],
            'references': [],
            'special_elements': []
        }

        # Find all sense elements
        senses = root.findall('.//sense')
        analysis['total_senses'] = len(senses)

        for sense in senses:
            # Analyze sense attributes
            sense_type = sense.get('type', 'standard')
            sense_n = sense.get('n', 'unspecified')

            if sense_type not in analysis['sense_types']:
                analysis['sense_types'][sense_type] = 0
            analysis['sense_types'][sense_type] += 1

            # Get sense text
            sense_text = sense.text or ""
            if sense_text.strip():
                analysis['sense_markers'].append({
                    'type': sense_type,
                    'n': sense_n,
                    'text': sense_text[:100] + "..." if len(sense_text) > 100 else sense_text
                })

        # Find references
        refs = root.findall('.//ref')
        for ref in refs:
            analysis['references'].append({
                'target': ref.get('target'),
                'type': ref.get('type'),
                'cref': ref.get('cref'),
                'text': ref.text
            })

        # Find special elements
        special_tags = ['foreign', 'hi', 'quote', 'bibl', 'title']
        for tag in special_tags:
            elements = root.findall(f'.//{tag}')
            for elem in elements:
                analysis['special_elements'].append({
                    'tag': tag,
                    'lang': elem.get('lang'),
                    'rend': elem.get('rend'),
                    'text': elem.text[:50] + "..." if elem.text and len(elem.text) > 50 else elem.text
                })

        return analysis

    except ET.ParseError as e:
        return {'error': str(e)}

def find_citation_patterns(xml_string):
    """Step 5: Identify citation and reference patterns"""
    import re

    patterns = {
        'quranic': [],
        'poetry': [],
        'proverbs': [],
        'tropical_markers': []
    }

    # Quranic patterns
    qur_patterns = [
        r'\\[in the Kur ([^\\]]+)\\]',
        r'in the Kur \\[([^\\]]+)\\]',
        r'Kur[^.]{0,20}ch\\. [ivxlc]+',
        r'verse.{0,20}Kur'
    ]

    for pattern in qur_patterns:
        matches = re.finditer(pattern, xml_string, re.IGNORECASE)
        for match in matches:
            patterns['quranic'].append({
                'pattern': pattern,
                'match': match.group(0),
                'context': xml_string[max(0, match.start()-50):match.end()+50]
            })

    # Poetry patterns
    poetry_patterns = [
        r'verse of [A-Z][^.,]+',
        r'poet.{0,30}says?',
        r'poetry.{0,50}',
        r'metre.{0,30}'
    ]

    for pattern in poetry_patterns:
        matches = re.finditer(pattern, xml_string, re.IGNORECASE)
        for match in matches:
            patterns['poetry'].append({
                'pattern': pattern,
                'match': match.group(0),
                'context': xml_string[max(0, match.start()-50):match.end()+50]
            })

    # Tropical/signification markers
    trop_patterns = [
        r'tropical[^.]*',
        r'contrary[^.]*',
        r'metaphor[^.]*',
        r'figurative[^.]*'
    ]

    for pattern in trop_patterns:
        matches = re.finditer(pattern, xml_string, re.IGNORECASE)
        for match in matches:
            patterns['tropical_markers'].append({
                'pattern': pattern,
                'match': match.group(0),
                'context': xml_string[max(0, match.start()-50):match.end()+50]
            })

    return patterns

def main():
    print("🔍 TRACING LARGEST LANE'S LEXICON ENTRIES")
    print("=" * 70)
    print("Step-by-step analysis: Neo4j → SQL → XML → Patterns\\n")

    # Connect to SQL database
    sql_conn = sqlite3.connect("/Users/omaribrahim/Downloads/LexiconDatabase-1.0.9/lexicon.sqlite")

    try:
        with neo4j_driver.session() as session:
            print("📊 STEP 1: Query Neo4j for largest Word nodes")
            print("-" * 50)

            largest_words = session.execute_read(get_largest_word_nodes, 5)

            print(f"Found {len(largest_words)} largest Word nodes:")
            for i, (arabic, entry_id, data_size, definitions) in enumerate(largest_words, 1):
                def_preview = definitions[:100] + "..." if definitions and len(definitions) > 100 else definitions
                print(f"{i}. {arabic} | Entry ID: {entry_id} | Size: {data_size}")
                print(f"   Definitions preview: {def_preview}\\n")

            # Analyze the top 3 entries in detail
            print("\\n" + "=" * 70)
            print("📋 STEP 2-5: Detailed Analysis of Top 3 Entries")
            print("=" * 70)

            for i, (arabic, entry_id, data_size, definitions) in enumerate(largest_words[:3], 1):
                print(f"\\n🔍 ANALYSIS {i}: {arabic} (Entry ID: {entry_id})")
                print("=" * 50)

                # Step 2: Get SQL entry
                print("\\n📊 STEP 2: SQL Database Lookup")
                sql_entry = get_sql_entry_by_id(sql_conn, entry_id)

                if sql_entry:
                    entry_id_sql, root, word, bword, xml, page, nodenum = sql_entry
                    print(f"✅ Found in SQL: {word} (Root: {root}, Page: {page})")
                    print(f"   XML length: {len(xml):,} characters")

                    # Step 3: Pretty-print XML sample
                    print("\\n📋 STEP 3: XML Structure Sample (first 2000 chars)")
                    pretty_xml = pretty_print_xml(xml)
                    print(pretty_xml[:2000] + "\\n... [TRUNCATED]" if len(pretty_xml) > 2000 else pretty_xml)

                    # Step 4: Analyze sense structure
                    print("\\n🏗️ STEP 4: Sense Hierarchy Analysis")
                    sense_analysis = analyze_sense_structure(xml)

                    if 'error' not in sense_analysis:
                        print(f"Total senses: {sense_analysis['total_senses']}")
                        print(f"Sense types: {sense_analysis['sense_types']}")

                        if sense_analysis['sense_markers']:
                            print("\\nSense markers:")
                            for marker in sense_analysis['sense_markers'][:5]:
                                print(f"  - {marker['type']}/{marker['n']}: {marker['text']}")

                        if sense_analysis['references']:
                            print(f"\\nReferences found: {len(sense_analysis['references'])}")
                            for ref in sense_analysis['references'][:3]:
                                print(f"  - Target: {ref['target']}, Type: {ref['type']}")

                        if sense_analysis['special_elements']:
                            print(f"\\nSpecial elements: {len(sense_analysis['special_elements'])}")
                            element_counts = {}
                            for elem in sense_analysis['special_elements']:
                                tag = elem['tag']
                                element_counts[tag] = element_counts.get(tag, 0) + 1
                            print(f"  Element counts: {element_counts}")

                    # Step 5: Find citation patterns
                    print("\\n🎯 STEP 5: Citation Pattern Detection")
                    citation_patterns = find_citation_patterns(xml)

                    for pattern_type, matches in citation_patterns.items():
                        if matches:
                            print(f"\\n{pattern_type.upper()} patterns found: {len(matches)}")
                            for match in matches[:2]:
                                print(f"  - {match['match']}")
                                print(f"    Context: ...{match['context']}...")

                else:
                    print(f"❌ Entry ID {entry_id} not found in SQL database")

                print("\\n" + "-" * 50)

        print("\\n" + "=" * 70)
        print("✅ ANALYSIS COMPLETE!")
        print("\\n📋 NEXT STEPS:")
        print("1. Choose extraction patterns based on findings")
        print("2. Design Neo4j schema for senses and references")
        print("3. Implement SQL → Neo4j ETL pipeline")
        print("4. Build targeted regex extractors")

    finally:
        sql_conn.close()
        neo4j_driver.close()

if __name__ == "__main__":
    main()