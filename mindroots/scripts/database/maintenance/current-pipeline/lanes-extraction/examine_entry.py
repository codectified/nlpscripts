"""
Examine a Specific Entry Line by Line
===================================

Pull raw XML for a specific entry and examine it to understand
what we actually need to extract and how to structure it properly.
"""

import os
import sqlite3
from xml.etree import ElementTree as ET
from xml.dom import minidom

def get_entry_xml(entry_id):
    """Get raw XML for a specific entry"""
    conn = sqlite3.connect("/Users/omaribrahim/Downloads/LexiconDatabase-1.0.9/lexicon.sqlite")
    cursor = conn.cursor()
    cursor.execute("SELECT id, word, root, xml FROM entry WHERE nodeid = ?", (entry_id,))
    result = cursor.fetchone()
    conn.close()
    return result

def pretty_print_xml(xml_string):
    """Pretty print XML for analysis"""
    try:
        root = ET.fromstring(xml_string)
        rough_string = ET.tostring(root, encoding='unicode')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="  ")
    except Exception as e:
        return f"Error parsing XML: {e}\n\nRaw XML:\n{xml_string}"

def analyze_entry_structure(xml_string):
    """Analyze the XML structure to understand sense markers"""
    try:
        root = ET.fromstring(xml_string)

        print("🔍 SENSE ELEMENTS ANALYSIS:")
        print("-" * 40)

        sense_elements = root.findall('.//sense')
        if sense_elements:
            for i, sense in enumerate(sense_elements, 1):
                print(f"Sense {i}:")
                print(f"  Type: {sense.get('type')}")
                print(f"  N: {sense.get('n')}")
                print(f"  Text: '{sense.text}'")
                print(f"  Tail: '{sense.tail[:100] if sense.tail else None}{'...' if sense.tail and len(sense.tail) > 100 else ''}'")
                print()
        else:
            print("No sense elements found")

        print("🏷️ ALL ELEMENT TYPES:")
        print("-" * 40)
        element_counts = {}
        for elem in root.iter():
            tag = elem.tag
            element_counts[tag] = element_counts.get(tag, 0) + 1

        for tag, count in sorted(element_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {tag}: {count}")

    except Exception as e:
        print(f"Error analyzing structure: {e}")

def main():
    # Good test entries - pick one
    test_entries = {
        "n1559": "أَىٌّ (6 senses - manageable)",
        "n1045": "إِلَّا (5 senses - simple)",
        "n25689": "ضَرَبَهُ (47 senses - complex)",
        "n31285": "عَيْنٌ (45 senses - classic)"
    }

    print("📋 AVAILABLE TEST ENTRIES:")
    for entry_id, desc in test_entries.items():
        print(f"  {entry_id}: {desc}")

    # Let's start with n1559 (أَىٌّ) - 6 senses
    entry_id = "n1559"

    print(f"\n🎯 EXAMINING ENTRY: {entry_id}")
    print("=" * 60)

    result = get_entry_xml(entry_id)
    if result:
        sql_id, word, root, xml = result
        print(f"SQL ID: {sql_id}")
        print(f"Word: {word}")
        print(f"Root: {root}")
        print(f"XML Length: {len(xml):,} characters")

        print(f"\n📋 PRETTY-PRINTED XML:")
        print("=" * 60)
        pretty_xml = pretty_print_xml(xml)
        print(pretty_xml)

        print(f"\n📊 STRUCTURE ANALYSIS:")
        print("=" * 60)
        analyze_entry_structure(xml)

        print(f"\n🎯 NEXT STEPS:")
        print("=" * 60)
        print("1. Look at the pretty-printed XML above")
        print("2. Identify where b1, b2, A1, A2 markers appear")
        print("3. See how senses are actually structured")
        print("4. Decide what fields we actually need")
        print("5. Design a better extraction strategy")

    else:
        print(f"❌ Entry {entry_id} not found in database")

if __name__ == "__main__":
    main()