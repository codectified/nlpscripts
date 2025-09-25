"""
Output Example Definition Node Structure
=======================================

Show exactly what a Definition node would look like with the new approach
so you can evaluate if this is what you want.
"""

import os
import sqlite3
import re
from xml.etree import ElementTree as ET
import json

def extract_sense_divisions(xml_string):
    """Extract properly structured sense divisions from XML"""
    try:
        root = ET.fromstring(xml_string)
        sense_divisions = []
        sense_elements = root.findall('.//sense')

        if not sense_elements:
            # No sense divisions - return whole content as single definition
            full_text = get_clean_text(root)
            return [{
                'sense_id': 'primary',
                'sense_type': 'primary',
                'marker': None,
                'definition_text': full_text[:1000],  # First 1000 chars
                'order': 1
            }]

        # Extract sense divisions
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
            definition_text = sense.tail.strip() if sense.tail else ""

            sense_divisions.append({
                'sense_id': sense_id,
                'sense_type': sense_type,
                'sense_number': sense_n,
                'marker': marker_text,
                'definition_text': definition_text[:500] if definition_text else "",
                'order': i + 1
            })

        return sense_divisions

    except ET.ParseError as e:
        return [{'error': f"XML Parse Error: {e}"}]

def get_clean_text(element):
    """Extract clean text from XML element"""
    text_parts = []
    if element.text:
        text_parts.append(element.text.strip())

    for child in element:
        if child.text:
            text_parts.append(child.text.strip())
        if child.tail:
            text_parts.append(child.tail.strip())

    return ' '.join(text_parts).strip()

def extract_arabic_elements(xml_string):
    """Extract Arabic text elements"""
    try:
        root = ET.fromstring(xml_string)
        arabic_elements = []

        for foreign in root.findall('.//foreign[@lang="ar"]'):
            if foreign.text:
                normalized = normalize_arabic(foreign.text.strip())
                arabic_elements.append({
                    'original': foreign.text.strip(),
                    'normalized': normalized
                })

        return arabic_elements

    except ET.ParseError:
        return []

def extract_references(xml_string):
    """Extract cross-references"""
    try:
        root = ET.fromstring(xml_string)
        references = []

        for ref in root.findall('.//ref'):
            if ref.get('target'):
                references.append({
                    'target_word': ref.get('target'),
                    'reference_type': ref.get('type'),
                    'reference_id': ref.get('cref')
                })

        return references

    except ET.ParseError:
        return []

def normalize_arabic(text):
    """Basic Arabic normalization"""
    if not text:
        return ""

    import unicodedata
    normalized = unicodedata.normalize('NFKD', text)
    normalized = re.sub(r'[\u064B-\u0655\u0670]', '', normalized)
    normalized = re.sub(r'[أإآٱ]', 'ا', normalized)
    normalized = normalized.replace('ى', 'ي')
    return normalized.strip()

def create_example_definition_node(entry_id):
    """Create an example of what the new Definition node would look like"""

    # Get XML from database
    conn = sqlite3.connect("/Users/omaribrahim/Downloads/LexiconDatabase-1.0.9/lexicon.sqlite")
    cursor = conn.cursor()
    cursor.execute("SELECT id, word, root, xml FROM entry WHERE nodeid = ?", (entry_id,))
    result = cursor.fetchone()
    conn.close()

    if not result:
        return None

    sql_id, word, root, xml = result

    # Extract structured data
    sense_divisions = extract_sense_divisions(xml)
    arabic_elements = extract_arabic_elements(xml)
    references = extract_references(xml)

    # Create the Definition node structure
    definition_node = {
        # Basic metadata
        'entry_id': entry_id,
        'word': word,
        'root': root,
        'sql_id': sql_id,

        # Counts
        'sense_count': len([s for s in sense_divisions if 'error' not in s]),
        'arabic_count': len(arabic_elements),
        'reference_count': len(references),

        # Structured data (this would be stored as JSON in Neo4j)
        'sense_divisions': sense_divisions,
        'arabic_elements': arabic_elements[:10],  # Limit for readability
        'cross_references': references,

        # Quick access properties
        'has_secondary_senses': any(s.get('sense_type') == 'b' for s in sense_divisions),
        'has_alternative_senses': any(s.get('sense_type') == 'A' for s in sense_divisions),

        # First sense for quick preview
        'primary_definition': sense_divisions[0].get('definition_text', '') if sense_divisions else '',

        # XML preservation (optional)
        'xml_length': len(xml),
        'preserve_xml': False  # Set to True if you want to keep full XML
    }

    return definition_node

def main():
    print("📋 EXAMPLE DEFINITION NODE OUTPUT")
    print("=" * 60)

    # Create examples for different entry types
    test_entries = [
        ("n1559", "أَىٌّ (6 senses - manageable)"),
        ("n1045", "إِلَّا (5 senses - simple)")
    ]

    for entry_id, description in test_entries:
        print(f"\n🎯 EXAMPLE: {description}")
        print("-" * 50)

        definition_node = create_example_definition_node(entry_id)

        if definition_node:
            # Pretty print the structure
            print("Definition Node Structure:")
            print(json.dumps(definition_node, indent=2, ensure_ascii=False))
        else:
            print(f"❌ Could not create example for {entry_id}")

        print("\n" + "="*60)

    print("\n🤔 EVALUATION QUESTIONS:")
    print("1. Is this structure useful and readable?")
    print("2. Do you want the full XML preserved or just extracted data?")
    print("3. Are the sense divisions clear and meaningful?")
    print("4. Should we limit Arabic elements or keep all?")
    print("5. Is this better than the previous JSON dump approach?")

if __name__ == "__main__":
    main()