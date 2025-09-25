"""
Better Lane's Extraction - Focused on Proper Sense Parsing
========================================================

Based on examining the actual XML structure, create a proper extractor
that captures the real sense divisions (b1, b2, A1, A2, etc.) and
the definition text that follows each marker.
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

def extract_sense_divisions(xml_string):
    """Extract properly structured sense divisions from XML"""
    try:
        root = ET.fromstring(xml_string)

        # Get all text content and track sense markers
        sense_divisions = []

        # Method: Walk through the XML and capture text segments
        # between sense markers

        def extract_text_content(element):
            """Extract clean text from XML element, handling nested tags"""
            text_parts = []

            # Get element text
            if element.text:
                text_parts.append(element.text.strip())

            # Process children
            for child in element:
                child_text = extract_text_content(child)
                if child_text:
                    text_parts.append(child_text)

                # Get tail text after child
                if child.tail:
                    text_parts.append(child.tail.strip())

            return ' '.join(text_parts).strip()

        # Find all sense elements in order
        sense_elements = root.findall('.//sense')

        if not sense_elements:
            # No sense divisions - return whole content as single definition
            full_text = extract_text_content(root)
            return [{
                'sense_id': 'primary',
                'sense_type': 'primary',
                'marker': None,
                'definition_text': full_text,
                'order': 1
            }]

        # Get full text to work with
        full_xml_text = ET.tostring(root, encoding='unicode', method='xml')

        # Extract sense divisions by finding content between markers
        for i, sense in enumerate(sense_elements):
            sense_type = sense.get('type', 'primary')
            sense_n = sense.get('n', '')
            marker_text = sense.text.strip() if sense.text else ''

            # Build sense ID
            if sense_type and sense_n:
                sense_id = f"{sense_type}{sense_n}"
            elif sense_type:
                sense_id = sense_type
            else:
                sense_id = f"sense_{i+1}"

            # Get the definition text that follows this sense marker
            # This is tricky - we need to get text from this sense until the next sense
            definition_text = ""

            if sense.tail:
                # Start with tail text of the sense element
                definition_text = sense.tail.strip()

            # For now, let's use a simpler approach - just get the tail
            # TODO: Improve this to capture full text until next sense marker

            sense_divisions.append({
                'sense_id': sense_id,
                'sense_type': sense_type,
                'sense_number': sense_n,
                'marker': marker_text,
                'definition_text': definition_text[:500] if definition_text else "",  # Truncate for readability
                'order': i + 1,
                'is_primary': sense_type not in ['b', 'A'],
                'is_secondary': sense_type == 'b',
                'is_alternative': sense_type == 'A'
            })

        return sense_divisions

    except ET.ParseError as e:
        return [{'error': f"XML Parse Error: {e}"}]

def extract_arabic_elements(xml_string):
    """Extract Arabic text elements properly"""
    try:
        root = ET.fromstring(xml_string)
        arabic_elements = []

        for foreign in root.findall('.//foreign[@lang="ar"]'):
            if foreign.text:
                arabic_elements.append({
                    'text': foreign.text.strip(),
                    'normalized': normalize_arabic(foreign.text.strip())
                })

        return arabic_elements[:20]  # Limit for readability

    except ET.ParseError:
        return []

def extract_references(xml_string):
    """Extract cross-references properly"""
    try:
        root = ET.fromstring(xml_string)
        references = []

        for ref in root.findall('.//ref'):
            if ref.get('target'):
                references.append({
                    'target': ref.get('target'),
                    'type': ref.get('type'),
                    'id': ref.get('cref')
                })

        return references

    except ET.ParseError:
        return []

def normalize_arabic(text):
    """Basic Arabic normalization"""
    if not text:
        return ""

    import unicodedata

    # NFKD normalization
    normalized = unicodedata.normalize('NFKD', text)

    # Remove diacritics
    normalized = re.sub(r'[\u064B-\u0655\u0670]', '', normalized)

    # Normalize alifs
    normalized = re.sub(r'[أإآٱ]', 'ا', normalized)

    # Normalize ya
    normalized = normalized.replace('ى', 'ي')

    return normalized.strip()

def get_test_entry(entry_id):
    """Get XML for test entry"""
    conn = sqlite3.connect("/Users/omaribrahim/Downloads/LexiconDatabase-1.0.9/lexicon.sqlite")
    cursor = conn.cursor()
    cursor.execute("SELECT id, word, root, xml FROM entry WHERE nodeid = ?", (entry_id,))
    result = cursor.fetchone()
    conn.close()
    return result

def main():
    print("🎯 BETTER LANE'S EXTRACTION - FOCUSED APPROACH")
    print("=" * 60)

    # Test with manageable entry
    test_id = "n1559"  # أَىٌّ - 6 senses

    result = get_test_entry(test_id)
    if not result:
        print(f"❌ Entry {test_id} not found")
        return

    sql_id, word, root, xml = result
    print(f"Testing with: {word} ({test_id})")
    print(f"XML length: {len(xml):,} characters")

    print(f"\n📋 SENSE DIVISIONS:")
    print("-" * 40)

    sense_divisions = extract_sense_divisions(xml)

    for i, sense in enumerate(sense_divisions, 1):
        if 'error' in sense:
            print(f"❌ {sense['error']}")
            continue

        print(f"{i}. Sense ID: {sense['sense_id']}")
        print(f"   Type: {sense['sense_type']} | Number: {sense['sense_number']}")
        print(f"   Marker: '{sense['marker']}'")
        print(f"   Primary: {sense['is_primary']} | Secondary: {sense['is_secondary']} | Alternative: {sense['is_alternative']}")

        if sense['definition_text']:
            def_preview = sense['definition_text'][:100] + "..." if len(sense['definition_text']) > 100 else sense['definition_text']
            print(f"   Definition: {def_preview}")
        print()

    print(f"📚 ARABIC ELEMENTS:")
    print("-" * 40)

    arabic_elements = extract_arabic_elements(xml)
    for i, elem in enumerate(arabic_elements[:5], 1):
        print(f"{i}. {elem['text']} → {elem['normalized']}")

    if len(arabic_elements) > 5:
        print(f"   ... and {len(arabic_elements) - 5} more")

    print(f"\n🔗 CROSS-REFERENCES:")
    print("-" * 40)

    references = extract_references(xml)
    for i, ref in enumerate(references, 1):
        print(f"{i}. {ref['target']} (type: {ref['type']})")

    print(f"\n🎯 ASSESSMENT:")
    print("-" * 40)
    print(f"✅ Found {len(sense_divisions)} sense divisions")
    print(f"✅ Found {len(arabic_elements)} Arabic elements")
    print(f"✅ Found {len(references)} cross-references")

    print(f"\n🔧 IMPROVEMENTS NEEDED:")
    print("1. Better text extraction between sense markers")
    print("2. Handle nested XML elements in definitions")
    print("3. Preserve formatting markers (<hi>, <foreign>, etc.)")
    print("4. Extract full definition text, not just tail")

if __name__ == "__main__":
    main()