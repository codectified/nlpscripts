"""
Lane's Lexicon SQL Database Explorer
==================================

Analyzes the SQL database to identify markup patterns for:
- Poetry and verse citations
- Proverbs and sayings
- Quranic references
- Contrary/tropical significations
- Cross-references and sources

This will help design the optimal extraction strategy.
"""

import sqlite3
import re
from xml.etree import ElementTree as ET
from collections import defaultdict, Counter

def connect_to_lanes_db():
    """Connect to Lane's Lexicon SQLite database"""
    return sqlite3.connect("/Users/omaribrahim/Downloads/LexiconDatabase-1.0.9/lexicon.sqlite")

def search_entries_by_pattern(conn, search_term, limit=20):
    """Search for entries containing specific patterns"""
    cursor = conn.cursor()
    query = """
    SELECT id, root, word, xml
    FROM entry
    WHERE xml LIKE ?
    LIMIT ?
    """
    cursor.execute(query, (f'%{search_term}%', limit))
    return cursor.fetchall()

def analyze_xml_structure(xml_content):
    """Parse XML and analyze structure for special markings"""
    try:
        root = ET.fromstring(xml_content)
        analysis = {
            'tags': set(),
            'attributes': defaultdict(set),
            'sense_markers': [],
            'foreign_text': [],
            'special_elements': []
        }

        # Recursively analyze all elements
        def analyze_element(elem, path=""):
            current_path = f"{path}/{elem.tag}" if path else elem.tag
            analysis['tags'].add(elem.tag)

            # Collect attributes
            for attr, value in elem.attrib.items():
                analysis['attributes'][f"{elem.tag}@{attr}"].add(value)

            # Look for special content
            if elem.text:
                text = elem.text.strip()
                if text:
                    # Look for sense markers
                    if re.search(r'-[A-Za-z]?\d+-', text):
                        analysis['sense_markers'].append(text[:50])

                    # Look for Arabic text (foreign elements)
                    if elem.tag == 'foreign' and 'lang="ar"' in ET.tostring(elem, encoding='unicode'):
                        analysis['foreign_text'].append(text)

            # Special element types
            if elem.tag in ['ref', 'quote', 'cit', 'bibl', 'title']:
                analysis['special_elements'].append({
                    'tag': elem.tag,
                    'attrs': dict(elem.attrib),
                    'text': elem.text.strip() if elem.text else None,
                    'path': current_path
                })

            # Recurse into children
            for child in elem:
                analyze_element(child, current_path)

        analyze_element(root)
        return analysis

    except ET.ParseError as e:
        return {'error': str(e)}

def search_poetry_patterns(conn):
    """Search for poetry-related markup patterns"""
    print("🎭 POETRY & VERSE PATTERNS")
    print("=" * 50)

    poetry_terms = [
        'verse', 'poetry', 'poet', 'metre', 'rajaz', 'basīt', 'kāmil'
    ]

    for term in poetry_terms:
        entries = search_entries_by_pattern(conn, term, 5)
        if entries:
            print(f"\\n📝 Entries containing '{term}': {len(entries)}")
            for entry_id, root, word, xml in entries[:2]:
                print(f"   Word: {word} (Root: {root})")
                # Show relevant snippet
                xml_lower = xml.lower()
                start = max(0, xml_lower.find(term.lower()) - 100)
                end = min(len(xml), xml_lower.find(term.lower()) + 200)
                snippet = xml[start:end].replace('\\n', ' ')
                print(f"   Context: ...{snippet}...")
                break

def search_proverb_patterns(conn):
    """Search for proverb-related markup patterns"""
    print("\\n\\n🗣️ PROVERB PATTERNS")
    print("=" * 50)

    proverb_terms = [
        'proverb', 'saying', 'maxim', 'adage', 'تمثل', 'مثل'
    ]

    for term in proverb_terms:
        entries = search_entries_by_pattern(conn, term, 3)
        if entries:
            print(f"\\n📜 Entries containing '{term}': {len(entries)}")
            for entry_id, root, word, xml in entries[:1]:
                print(f"   Word: {word}")
                analysis = analyze_xml_structure(xml)
                if 'special_elements' in analysis:
                    for elem in analysis['special_elements'][:3]:
                        print(f"   Special: {elem}")

def search_quranic_patterns(conn):
    """Search for Quranic reference patterns"""
    print("\\n\\n📖 QURANIC REFERENCE PATTERNS")
    print("=" * 50)

    quranic_terms = [
        'Kur', 'Qur', 'القرآن', 'كتاب الله', 'revelation'
    ]

    for term in quranic_terms:
        entries = search_entries_by_pattern(conn, term, 5)
        if entries:
            print(f"\\n📕 Entries containing '{term}': {len(entries)}")
            for entry_id, root, word, xml in entries[:2]:
                print(f"   Word: {word}")
                # Look for citation patterns
                citation_patterns = re.findall(r'\\([^)]*' + re.escape(term) + r'[^)]*\\)', xml, re.IGNORECASE)
                for citation in citation_patterns[:2]:
                    print(f"   Citation: {citation}")

def search_signification_patterns(conn):
    """Search for contrary/tropical signification markers"""
    print("\\n\\n🔄 CONTRARY/TROPICAL SIGNIFICATION PATTERNS")
    print("=" * 50)

    signification_terms = [
        'tropical', 'contrary', 'metaphor', 'figurative', 'contr.', 'trop.'
    ]

    for term in signification_terms:
        entries = search_entries_by_pattern(conn, term, 5)
        if entries:
            print(f"\\n🔀 Entries containing '{term}': {len(entries)}")
            for entry_id, root, word, xml in entries[:2]:
                print(f"   Word: {word}")
                # Analyze XML structure for sense markers
                analysis = analyze_xml_structure(xml)
                if 'sense_markers' in analysis:
                    print(f"   Sense markers: {analysis['sense_markers'][:3]}")

def analyze_sense_structure(conn):
    """Analyze how senses are structured in the XML"""
    print("\\n\\n🏗️ SENSE STRUCTURE ANALYSIS")
    print("=" * 50)

    cursor = conn.cursor()
    cursor.execute("SELECT xml FROM entry WHERE xml LIKE '%sense%' LIMIT 10")

    all_tags = Counter()
    all_attributes = Counter()

    for (xml,) in cursor.fetchall():
        analysis = analyze_xml_structure(xml)
        if 'error' not in analysis:
            all_tags.update(analysis['tags'])
            for attr_combo, values in analysis['attributes'].items():
                all_attributes[attr_combo] += len(values)

    print("\\n📊 Most common XML tags:")
    for tag, count in all_tags.most_common(15):
        print(f"   {tag}: {count}")

    print("\\n📊 Most common attributes:")
    for attr, count in all_attributes.most_common(10):
        print(f"   {attr}: {count}")

def sample_complex_entries(conn):
    """Sample entries with complex structure"""
    print("\\n\\n🧩 COMPLEX ENTRY SAMPLES")
    print("=" * 50)

    # Look for entries with multiple sense markers
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, root, word, xml
        FROM entry
        WHERE xml LIKE '%-A2-%' AND xml LIKE '%-b2-%'
        LIMIT 3
    """)

    for entry_id, root, word, xml in cursor.fetchall():
        print(f"\\n📚 Complex Entry: {word} (Root: {root})")
        print("-" * 30)

        analysis = analyze_xml_structure(xml)
        if 'error' not in analysis:
            print(f"Tags: {list(analysis['tags'])}")
            print(f"Sense markers found: {len(analysis['sense_markers'])}")

            # Show first few sense markers
            for marker in analysis['sense_markers'][:3]:
                print(f"   Marker: {marker}")

def main():
    print("🔍 Lane's Lexicon SQL Database Deep Analysis")
    print("=" * 70)
    print("Analyzing markup patterns for fine-grained extraction...\\n")

    conn = connect_to_lanes_db()

    try:
        # Basic database stats
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM entry")
        total_entries = cursor.fetchone()[0]
        print(f"📊 Total entries in database: {total_entries:,}")

        # Run all analyses
        search_poetry_patterns(conn)
        search_proverb_patterns(conn)
        search_quranic_patterns(conn)
        search_signification_patterns(conn)
        analyze_sense_structure(conn)
        sample_complex_entries(conn)

        print("\\n" + "=" * 70)
        print("✅ Analysis complete! Use findings to design extraction strategy.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()