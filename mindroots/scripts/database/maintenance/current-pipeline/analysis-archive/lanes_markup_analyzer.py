"""
Detailed Lane's Lexicon Markup Pattern Analyzer
==============================================

Deep dive into specific XML structures to understand exact markup for:
- Poetry citations and verse references
- Proverb markers and folk sayings
- Quranic citations with chapter/verse
- Contrary/tropical sense markers
- Cross-references and bibliographic citations
"""

import sqlite3
import re
from xml.etree import ElementTree as ET

def connect_to_lanes_db():
    return sqlite3.connect("/Users/omaribrahim/Downloads/LexiconDatabase-1.0.9/lexicon.sqlite")

def pretty_print_xml(xml_string, max_length=1000):
    """Pretty print XML with truncation"""
    try:
        root = ET.fromstring(xml_string)
        pretty = ET.tostring(root, encoding='unicode', method='xml')
        if len(pretty) > max_length:
            return pretty[:max_length] + "\\n... [TRUNCATED]"
        return pretty
    except:
        return xml_string[:max_length] + "... [TRUNCATED]" if len(xml_string) > max_length else xml_string

def analyze_quranic_citations(conn):
    """Deep analysis of Quranic citation patterns"""
    print("📖 QURANIC CITATION DEEP ANALYSIS")
    print("=" * 60)

    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, root, word, xml
        FROM entry
        WHERE xml LIKE '%Kur%' OR xml LIKE '%قرآن%'
        LIMIT 5
    """)

    for i, (entry_id, root, word, xml) in enumerate(cursor.fetchall(), 1):
        print(f"\\n{i}. WORD: {word} (Root: {root})")
        print("-" * 40)

        # Look for Quranic citation patterns
        kur_matches = re.finditer(r'[^>]*Kur[^<]*', xml, re.IGNORECASE)
        for match in list(kur_matches)[:3]:
            citation = match.group(0).strip()
            print(f"Kur Citation: {citation}")

        # Look for verse/chapter references
        verse_patterns = [
            r'ch\\. [ivxlc]+',  # chapter references
            r'verse \\d+',       # verse numbers
            r'\\d+:\\d+',        # chapter:verse format
        ]

        for pattern in verse_patterns:
            matches = re.findall(pattern, xml, re.IGNORECASE)
            if matches:
                print(f"Verse refs ({pattern}): {matches[:3]}")

        # Show XML structure around Quranic references
        if 'Kur' in xml:
            kur_pos = xml.lower().find('kur')
            start = max(0, kur_pos - 200)
            end = min(len(xml), kur_pos + 300)
            context = xml[start:end]
            print(f"XML Context: {context}")

def analyze_poetry_citations(conn):
    """Deep analysis of poetry citation markup"""
    print("\\n\\n🎭 POETRY CITATION DEEP ANALYSIS")
    print("=" * 60)

    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, root, word, xml
        FROM entry
        WHERE xml LIKE '%poet%' OR xml LIKE '%verse%' OR xml LIKE '%metre%'
        LIMIT 3
    """)

    for i, (entry_id, root, word, xml) in enumerate(cursor.fetchall(), 1):
        print(f"\\n{i}. WORD: {word}")
        print("-" * 30)

        # Look for poet names and attribution
        poet_patterns = [
            r'[A-Z][a-z]+-[a-z]+-[A-Z][a-z]+',  # Imra-el-Keys pattern
            r'[A-Z][a-z]+ says?',                # Attribution patterns
            r'poet says?',
            r'in a verse of [^,]+'
        ]

        for pattern in poet_patterns:
            matches = re.findall(pattern, xml)
            if matches:
                print(f"Poet attribution: {matches}")

        # Look for verse markers and citations
        if 'verse' in xml.lower():
            verse_context = re.findall(r'[^.]*verse[^.]*', xml, re.IGNORECASE)
            for context in verse_context[:2]:
                print(f"Verse context: {context.strip()}")

def analyze_proverb_patterns(conn):
    """Deep analysis of proverb and saying markup"""
    print("\\n\\n🗣️ PROVERB & SAYING DEEP ANALYSIS")
    print("=" * 60)

    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, root, word, xml
        FROM entry
        WHERE xml LIKE '%proverb%' OR xml LIKE '%saying%' OR xml LIKE '%مثل%'
        LIMIT 3
    """)

    for i, (entry_id, root, word, xml) in enumerate(cursor.fetchall(), 1):
        print(f"\\n{i}. WORD: {word}")
        print("-" * 30)

        # Look for proverb markers
        proverb_patterns = [
            r'[Ii]t is a proverb',
            r'[Aa] proverb',
            r'[Aa] saying',
            r'[Pp]roverbial',
            r'مثل',
            r'قولهم'
        ]

        for pattern in proverb_patterns:
            matches = re.findall(r'[^.]*' + pattern + r'[^.]*', xml, re.IGNORECASE)
            for match in matches[:2]:
                print(f"Proverb marker: {match.strip()}")

def analyze_sense_markers(conn):
    """Detailed analysis of sense marker patterns"""
    print("\\n\\n🔍 SENSE MARKER DETAILED ANALYSIS")
    print("=" * 60)

    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, root, word, xml
        FROM entry
        WHERE xml LIKE '%-A2-%' AND xml LIKE '%-b2-%'
        LIMIT 2
    """)

    for i, (entry_id, root, word, xml) in enumerate(cursor.fetchall(), 1):
        print(f"\\n{i}. WORD: {word} (ID: {entry_id})")
        print("-" * 40)

        # Parse XML to understand sense structure
        try:
            root_elem = ET.fromstring(xml)

            # Find sense elements
            sense_elements = root_elem.findall('.//sense')
            print(f"Found {len(sense_elements)} sense elements:")

            for j, sense in enumerate(sense_elements[:4]):
                attrs = sense.attrib
                text = sense.text or ""
                print(f"  Sense {j+1}: type='{attrs.get('type', 'N/A')}', n='{attrs.get('n', 'N/A')}'")
                print(f"    Text: {text[:100]}{'...' if len(text) > 100 else ''}")

        except ET.ParseError:
            print("  (XML parse error - analyzing text patterns)")

        # Look for inline sense markers in text
        sense_markers = re.findall(r'-[A-Za-z]?\d+-', xml)
        marker_contexts = []

        for marker in set(sense_markers[:6]):
            # Get context around each marker
            marker_pos = xml.find(marker)
            if marker_pos >= 0:
                start = max(0, marker_pos - 100)
                end = min(len(xml), marker_pos + 150)
                context = xml[start:end].replace('\\n', ' ')
                marker_contexts.append((marker, context))

        print(f"\\nSense markers in text: {set(sense_markers)}")
        for marker, context in marker_contexts[:3]:
            print(f"  {marker}: ...{context}...")

def analyze_contrary_tropical(conn):
    """Analyze contrary and tropical signification markers"""
    print("\\n\\n🔄 CONTRARY/TROPICAL SIGNIFICATION ANALYSIS")
    print("=" * 60)

    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, root, word, xml
        FROM entry
        WHERE xml LIKE '%tropical%' OR xml LIKE '%contrary%' OR xml LIKE '%contr.%'
        LIMIT 3
    """)

    for i, (entry_id, root, word, xml) in enumerate(cursor.fetchall(), 1):
        print(f"\\n{i}. WORD: {word}")
        print("-" * 30)

        # Look for specific signification markers
        signification_patterns = [
            r'\\btropical\\b[^.]*',
            r'\\bcontrary\\b[^.]*',
            r'\\bcontr\\.[^.]*',
            r'\\bmetaphor[^.]*',
            r'\\bfigurative[^.]*'
        ]

        for pattern in signification_patterns:
            matches = re.findall(pattern, xml, re.IGNORECASE)
            for match in matches[:2]:
                print(f"Signification: {match.strip()}")

        # Check if these are in sense elements or inline
        try:
            root_elem = ET.fromstring(xml)
            tropical_senses = root_elem.findall('.//sense[@type="tropical"]')
            contrary_senses = root_elem.findall('.//sense[@type="contrary"]')

            if tropical_senses:
                print(f"Found {len(tropical_senses)} tropical sense elements")
            if contrary_senses:
                print(f"Found {len(contrary_senses)} contrary sense elements")

        except ET.ParseError:
            pass

def main():
    print("🔬 Lane's Lexicon Detailed Markup Analysis")
    print("=" * 70)
    print("Examining specific XML structures and patterns...\\n")

    conn = connect_to_lanes_db()

    try:
        analyze_quranic_citations(conn)
        analyze_poetry_citations(conn)
        analyze_proverb_patterns(conn)
        analyze_sense_markers(conn)
        analyze_contrary_tropical(conn)

        print("\\n" + "=" * 70)
        print("✅ Detailed analysis complete!")
        print("\\n📋 KEY FINDINGS SUMMARY:")
        print("- Quranic refs: Look for 'Kur', chapter/verse patterns")
        print("- Poetry: Look for poet names, 'verse of', attribution")
        print("- Proverbs: Look for 'proverb', 'saying', Arabic مثل")
        print("- Senses: <sense> elements + inline -A2-, -b2- markers")
        print("- Significations: 'tropical', 'contrary', 'contr.' patterns")

    finally:
        conn.close()

if __name__ == "__main__":
    main()