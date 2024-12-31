from neo4j import GraphDatabase
import time

from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Get the Neo4j connection details from the environment
uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASS')

# Validate that the credentials are set
if not all([uri, user, password]):
    raise ValueError("Missing Neo4j connection details. Ensure NEO4J_URI, NEO4J_USER, and NEO4J_PASS are set in your .env file.")

# Connect to Neo4j
driver = GraphDatabase.driver(uri, auth=(user, password))

# Function to create or retrieve a Corpus node and attempt to link to CorpusItems with corpus_id: 3
def create_corpus_and_link_items(tx):
    # Create the Corpus node if it doesn't exist
    result = tx.run("""
        MERGE (c:Corpus {corpus_id: 3})
        ON CREATE SET 
            c.arabic = 'لامية العرب - للشنفرى', 
            c.english = 'Lamiyat al-Arab by Al-Shanfara'
        RETURN c
    """)
    corpus_node = result.single()
    if corpus_node:
        print("Corpus node created or retrieved:", corpus_node["c"])

    # Attempt to link any CorpusItem nodes with corpus_id: 3 to this Corpus node
    tx.run("""
        MATCH (c:Corpus {corpus_id: 3})
        MATCH (ci:CorpusItem {corpus_id: 3})
        MERGE (c)<-[:BELOGS_TO]-(ci)
    """)
    print("Attempted to link CorpusItem nodes with corpus_id: 3 to the Corpus node.")

# Main function with retry mechanism
def main():
    retries = 3  # Number of retries in case of connection issues
    for attempt in range(retries):
        try:
            with driver.session() as session:
                session.execute_write(create_corpus_and_link_items)
                print("Test completed successfully.")
            break  # Exit the retry loop if successful
        except Exception as e:
            print(f"Attempt {attempt + 1} failed:", e)
            time.sleep(5)  # Wait before retrying
    driver.close()  # Ensure the driver closes

if __name__ == "__main__":
    main()