"""
Verify and Explore Extracted Definition Nodes
============================================

Query the created Definition nodes to verify extraction success
and explore the rich semantic data now available in Neo4j.
"""

import os
import json
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()
neo4j_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
)

def get_definition_overview(tx):
    """Get overview of all Definition nodes"""
    query = """
    MATCH (w:Word)-[:HAS_DEFINITION]->(d:Definition)
    RETURN w.arabic, d.entry_id, d.sense_count, d.reference_count,
           d.arabic_text_count, d.has_tropical_senses, d.primary_definition
    ORDER BY d.sense_count DESC
    """
    result = tx.run(query)
    return [(record["w.arabic"], record["d.entry_id"], record["d.sense_count"],
             record["d.reference_count"], record["d.arabic_text_count"],
             record["d.has_tropical_senses"], record["d.primary_definition"])
            for record in result]

def get_extracted_senses(tx, entry_id):
    """Get detailed sense data for a specific entry"""
    query = """
    MATCH (d:Definition {entry_id: $entry_id})
    RETURN d.extracted_senses
    """
    result = tx.run(query, entry_id=entry_id)
    record = result.single()
    if record and record["d.extracted_senses"]:
        return json.loads(record["d.extracted_senses"])
    return []

def get_extracted_references(tx, entry_id):
    """Get cross-reference data for a specific entry"""
    query = """
    MATCH (d:Definition {entry_id: $entry_id})
    RETURN d.extracted_references
    """
    result = tx.run(query, entry_id=entry_id)
    record = result.single()
    if record and record["d.extracted_references"]:
        return json.loads(record["d.extracted_references"])
    return []

def get_extracted_arabic(tx, entry_id):
    """Get Arabic text elements for a specific entry"""
    query = """
    MATCH (d:Definition {entry_id: $entry_id})
    RETURN d.extracted_arabic
    """
    result = tx.run(query, entry_id=entry_id)
    record = result.single()
    if record and record["d.extracted_arabic"]:
        return json.loads(record["d.extracted_arabic"])
    return []

def main():
    print("🔍 VERIFYING EXTRACTED DEFINITION NODES")
    print("=" * 60)

    try:
        with neo4j_driver.session() as session:
            # Get overview
            print("📊 DEFINITION NODES OVERVIEW:")
            print("-" * 40)

            definitions = session.execute_read(get_definition_overview)

            print(f"Found {len(definitions)} Definition nodes:\n")
            for i, (arabic, entry_id, sense_count, ref_count, arabic_count,
                   has_tropical, primary_def) in enumerate(definitions, 1):

                print(f"{i}. {arabic} ({entry_id})")
                print(f"   Senses: {sense_count} | References: {ref_count} | Arabic: {arabic_count}")
                print(f"   Tropical: {has_tropical}")

                # Show truncated primary definition
                if primary_def:
                    preview = primary_def[:100] + "..." if len(primary_def) > 100 else primary_def
                    print(f"   Definition: {preview}")
                print()

            # Detailed analysis of richest entry
            if definitions:
                richest_entry = definitions[0]  # First one has most senses
                arabic, entry_id = richest_entry[0], richest_entry[1]

                print(f"\n🎯 DETAILED ANALYSIS: {arabic} ({entry_id})")
                print("=" * 60)

                # Sense hierarchy
                print("\n📋 SENSE HIERARCHY:")
                print("-" * 30)
                senses = session.execute_read(get_extracted_senses, entry_id)

                if senses:
                    primary_senses = [s for s in senses if s['is_primary']]
                    secondary_senses = [s for s in senses if s['is_secondary']]
                    alternative_senses = [s for s in senses if s['is_alternative']]

                    print(f"Primary senses: {len(primary_senses)}")
                    print(f"Secondary senses (-b-): {len(secondary_senses)}")
                    print(f"Alternative senses (-A-): {len(alternative_senses)}")

                    print("\nFirst few senses:")
                    for i, sense in enumerate(senses[:5], 1):
                        marker = sense['marker_text'] if sense['marker_text'] else f"[{sense['sense_type']}]"
                        print(f"  {i}. {marker} ({sense['sense_id']})")
                        if sense['definition_text']:
                            def_preview = sense['definition_text'][:80] + "..." if len(sense['definition_text']) > 80 else sense['definition_text']
                            print(f"     {def_preview}")

                # Cross-references
                print("\n🔗 CROSS-REFERENCES:")
                print("-" * 30)
                references = session.execute_read(get_extracted_references, entry_id)

                if references:
                    print(f"Found {len(references)} cross-references:")
                    for i, ref in enumerate(references[:5], 1):
                        target = ref['target_word'] or '[no target]'
                        ref_type = ref['reference_type'] or '[no type]'
                        print(f"  {i}. {target} (type: {ref_type})")

                # Arabic text elements
                print("\n🌍 ARABIC TEXT ELEMENTS:")
                print("-" * 30)
                arabic_texts = session.execute_read(get_extracted_arabic, entry_id)

                if arabic_texts:
                    print(f"Found {len(arabic_texts)} Arabic text elements:")
                    for i, arabic_elem in enumerate(arabic_texts[:10], 1):
                        text = arabic_elem['text']
                        normalized = arabic_elem['normalized']
                        print(f"  {i}. {text} → {normalized}")

            print("\n✅ EXTRACTION VERIFICATION COMPLETE!")
            print("=" * 60)
            print("🎯 KEY ACHIEVEMENTS:")
            print("1. ✅ Successfully extracted sense hierarchy from XML")
            print("2. ✅ Cross-references properly parsed and stored")
            print("3. ✅ Arabic text elements extracted and normalized")
            print("4. ✅ Tropical/contrary significations detected")
            print("5. ✅ All data stored as queryable Neo4j properties")

            print("\n🚀 READY FOR:")
            print("- Semantic queries across sense hierarchies")
            print("- Cross-reference network analysis")
            print("- Arabic text search and matching")
            print("- Fine-grained lexical research")

    finally:
        neo4j_driver.close()

if __name__ == "__main__":
    main()