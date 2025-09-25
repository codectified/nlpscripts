"""
Neo4j Schema Design for Lane's Lexicon Fine-Grained Data
======================================================

Based on XML analysis, design optimal Neo4j schema for sense hierarchy,
references, and semantic data.
"""

def print_schema_design():
    print("🏗️ NEO4J SCHEMA DESIGN FOR LANE'S LEXICON")
    print("=" * 60)

    print("\n📊 CORE STRUCTURE:")
    print("-" * 30)
    schema = """
    // Existing structure (keep as-is)
    (:Root)-[:HAS_WORD]->(:Word)

    // NEW: Fine-grained definition structure
    (:Word)-[:HAS_DEFINITION]->(:Definition)
    (:Definition)-[:HAS_SENSE]->(:Sense)
    (:Sense)-[:REFERENCES]->(:Word)
    (:Sense)-[:CONTAINS_ARABIC]->(:ArabicText)
    """
    print(schema)

    print("\n🎯 NODE TYPES & PROPERTIES:")
    print("-" * 30)

    nodes = {
        "Definition": {
            "purpose": "Container for all XML content from one Lane's entry",
            "properties": {
                "entry_id": "String (n31285)",
                "xml_length": "Integer",
                "total_senses": "Integer",
                "has_references": "Boolean",
                "has_arabic_text": "Boolean",
                "signification_types": "[String] (tropical, contrary, etc.)"
            }
        },

        "Sense": {
            "purpose": "Individual meaning/sense from XML <sense> elements",
            "properties": {
                "sense_id": "String (b2, A3, primary)",
                "sense_type": "String (secondary, alternative, primary)",
                "sense_number": "String (2, 3, 4)",
                "marker_text": "String (-b2-, -A2-)",
                "definition_text": "String (full text content)",
                "signification": "String (tropical, contrary, literal)",
                "order_in_entry": "Integer (for proper sequencing)"
            }
        },

        "ArabicText": {
            "purpose": "Arabic text from <foreign lang='ar'> elements",
            "properties": {
                "text": "String (original Arabic)",
                "normalized": "String (using unified normalization)",
                "context": "String (definition, example, etc.)",
                "element_type": "String (foreign, quote, etc.)"
            }
        },

        "CrossReference": {
            "purpose": "References from <ref> elements",
            "properties": {
                "target_word": "String",
                "reference_type": "String",
                "cross_ref_id": "String",
                "context": "String"
            }
        }
    }

    for node_type, details in nodes.items():
        print(f"\n({node_type}):")
        print(f"  Purpose: {details['purpose']}")
        print("  Properties:")
        for prop, desc in details['properties'].items():
            print(f"    {prop}: {desc}")

    print("\n🔗 RELATIONSHIP TYPES:")
    print("-" * 30)

    relationships = {
        "HAS_DEFINITION": "Word → Definition (one-to-one)",
        "HAS_SENSE": "Definition → Sense (one-to-many, ordered)",
        "REFERENCES": "Sense → Word (cross-references)",
        "CONTAINS_ARABIC": "Sense → ArabicText (embedded Arabic)",
        "HAS_REFERENCE": "Sense → CrossReference (structured refs)",
        "TROPICAL_OF": "Sense → Sense (signification relationships)",
        "CONTRARY_TO": "Sense → Sense (opposite meanings)"
    }

    for rel_type, desc in relationships.items():
        print(f"  :{rel_type} - {desc}")

def print_implementation_strategy():
    print("\n\n🚀 IMPLEMENTATION STRATEGY")
    print("=" * 60)

    print("\n📋 OPTION 1: SINGLE DEFINITION NODE (Recommended)")
    print("-" * 50)
    advantages = [
        "✅ Keep XML structure intact as properties",
        "✅ Easy to query: MATCH (w:Word)-[:HAS_DEFINITION]->(d)",
        "✅ Can extract senses on-demand or pre-compute",
        "✅ Preserves all XML data for future extraction",
        "✅ Faster implementation"
    ]

    for adv in advantages:
        print(f"  {adv}")

    example_definition = {
        "entry_id": "n31285",
        "xml_source": "<entryFree>...</entryFree>",
        "sense_count": 45,
        "reference_count": 17,
        "arabic_text_count": 289,
        "has_tropical_senses": True,
        "has_contrary_senses": False,
        "primary_definition": "The eye: organ of sight...",
        "extracted_senses": "[{sense_id: 'b2', text: '...'}, ...]",
        "extracted_references": "[{target: 'عُيَيْنَةٌ', type: '1'}, ...]"
    }

    print(f"\n  Example Definition node:")
    for key, value in example_definition.items():
        print(f"    {key}: {value}")

    print("\n📋 OPTION 2: FULL DECOMPOSITION")
    print("-" * 50)
    print("  - Create separate Sense, Reference, ArabicText nodes")
    print("  - More normalized but complex queries")
    print("  - Better for complex sense relationship analysis")
    print("  - Slower to implement and populate")

def print_extraction_workflow():
    print("\n\n⚙️ EXTRACTION WORKFLOW")
    print("=" * 60)

    workflow = """
    1. Query SQL: SELECT nodeid, xml FROM entry WHERE nodeid IN (neo4j_entry_ids)

    2. For each XML:
       - Parse with ElementTree
       - Extract sense hierarchy
       - Extract references
       - Extract Arabic text
       - Classify significations

    3. Create Neo4j Definition node:
       - Store raw XML + extracted properties
       - Link to existing Word node

    4. Optionally create detailed Sense nodes:
       - Individual sense definitions
       - Cross-references between senses
       - Arabic text associations
    """

    print(workflow)

def print_query_examples():
    print("\n\n🔍 QUERY EXAMPLES")
    print("=" * 60)

    queries = {
        "Find all tropical senses": """
        MATCH (w:Word)-[:HAS_DEFINITION]->(d:Definition)
        WHERE d.has_tropical_senses = true
        RETURN w.arabic, d.sense_count
        """,

        "Get sense hierarchy for a word": """
        MATCH (w:Word {arabic: 'عَيْنٌ'})-[:HAS_DEFINITION]->(d)
        RETURN d.extracted_senses
        """,

        "Find words with most cross-references": """
        MATCH (w:Word)-[:HAS_DEFINITION]->(d:Definition)
        WHERE d.reference_count > 10
        RETURN w.arabic, d.reference_count
        ORDER BY d.reference_count DESC
        """,

        "Find words with Arabic examples": """
        MATCH (w:Word)-[:HAS_DEFINITION]->(d)
        WHERE d.arabic_text_count > 50
        RETURN w.arabic, d.arabic_text_count
        """
    }

    for desc, query in queries.items():
        print(f"\n{desc}:")
        print(query)

if __name__ == "__main__":
    print_schema_design()
    print_implementation_strategy()
    print_extraction_workflow()
    print_query_examples()

    print("\n" + "="*60)
    print("🎯 RECOMMENDATION: Start with Option 1 (Single Definition Node)")
    print("✅ Fast to implement, preserves all data, easy to query")
    print("🚀 Can evolve to full decomposition later if needed")