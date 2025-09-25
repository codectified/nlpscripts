"""
Sample Neo4j Word node definitions to understand current text structure
"""

import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
)

def get_word_properties(tx):
    """Discover what properties Word nodes actually have"""
    query = """
    MATCH (w:Word)
    UNWIND keys(w) AS property_name
    RETURN DISTINCT property_name
    ORDER BY property_name
    """
    result = tx.run(query)
    return [record["property_name"] for record in result]

def get_sample_definitions(tx, limit=5):
    """Get sample Word node data to analyze structure"""
    query = """
    MATCH (w:Word)
    WHERE w.definitions IS NOT NULL
    RETURN w
    LIMIT 5
    """
    result = tx.run(query)
    return [dict(record["w"]) for record in result]

def main():
    print("📖 Neo4j Word node analysis:")
    print("=" * 70)

    with driver.session() as session:
        # First discover what properties exist
        print("\\n🔍 Word node properties:")
        properties = session.execute_read(get_word_properties)
        for prop in properties:
            print(f"  - {prop}")

        # Get sample Word data
        print("\\n📋 Sample Word nodes:")
        samples = session.execute_read(get_sample_definitions)

        for i, word_data in enumerate(samples, 1):
            print(f"\\n{i}. WORD NODE:")
            print("-" * 30)
            for key, value in word_data.items():
                if value and len(str(value)) > 100:
                    preview = str(value)[:200] + "..."
                    print(f"  {key}: {preview}")
                else:
                    print(f"  {key}: {value}")

            # Look for definition-like text in any property
            definition_text = None
            for key, value in word_data.items():
                if isinstance(value, str) and len(value) > 100:
                    # Look for sense markers
                    if "-b2-" in value or "-A2-" in value:
                        print(f"🔍 {key} CONTAINS SENSE MARKERS (-b2-, -A2-, etc.)")
                        definition_text = value

                    # Look for sources in parentheses
                    import re
                    sources = re.findall(r"\\([A-Z][A-Za-z., ]+\\)", value)
                    if sources:
                        print(f"🔍 {key} CONTAINS SOURCES: {sources[:3]}{'...' if len(sources) > 3 else ''}")

                    # Look for cross-references
                    crossrefs = re.findall(r"see art\\. ([\\u0621-\\u064A]+)", value)
                    if crossrefs:
                        print(f"🔍 {key} CONTAINS CROSS-REFS: {crossrefs}")

    driver.close()

if __name__ == "__main__":
    main()