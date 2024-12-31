from py2neo import Graph
import re
import csv

# Connect to Neo4j
graph = Graph("neo4j+s://0cbfce87.databases.neo4j.io", auth=("neo4j", "WesStKZQAAf8rBZ6AJVQJnnP7t8WTlPVPQK2mZnmSKw"))




# List of corpus items and word IDs to link
links = [
    {"corpus_item_id": 22, "word_ids": [17243]},
    {"corpus_item_id": 23, "word_ids": [42532, 47894]},
    {"corpus_item_id": 25, "word_ids": [22071]},
    {"corpus_item_id": 34, "word_ids": [24304]},
    {"corpus_item_id": 42, "word_ids": [17395]},
    {"corpus_item_id": 49, "word_ids": [24704]},
    {"corpus_item_id": 50, "word_ids": [9140, 9150, 9151, 9152, 9251]},
    {"corpus_item_id": 59, "word_ids": [10344]},
    {"corpus_item_id": 61, "word_ids": [10318, 10319, 10320, 40965, 40968]},
    {"corpus_item_id": 66, "word_ids": [350, 7778]},
    {"corpus_item_id": 67, "word_ids": [26493, 26497, 26498]},
    {"corpus_item_id": 70, "word_ids": [37740]},
    {"corpus_item_id": 71, "word_ids": [409]},
    {"corpus_item_id": 72, "word_ids": [1591, 46390]},
    {"corpus_item_id": 74, "word_ids": [29503]},
    {"corpus_item_id": 78, "word_ids": [2257, 2263, 2264, 2265]},
    {"corpus_item_id": 79, "word_ids": [4433]},
    {"corpus_item_id": 81, "word_ids": [31691, 31692]},
    {"corpus_item_id": 82, "word_ids": [15251]},
    {"corpus_item_id": 90, "word_ids": [27325, 27485]},
    {"corpus_item_id": 92, "word_ids": [45264, 45269, 45270]},
    {"corpus_item_id": 97, "word_ids": [16606]},
    {"corpus_item_id": 98, "word_ids": [25228]},
]

# Loop through each link item
for link in links:
    corpus_item_id = link["corpus_item_id"]
    word_ids = link["word_ids"]

    for word_id in word_ids:
        # Construct the Cypher query
        query = f"""
        MATCH (c:CorpusItem {{item_id: {corpus_item_id}}}), (w:Word {{word_id: {word_id}}})
        MERGE (c)-[:HAS_WORD]->(w)
        """
        
        # Run the query
        graph.run(query)

        # Output to console for logging
        print(f"Linked CorpusItem {corpus_item_id} with Word {word_id}")

print("Processing complete.")