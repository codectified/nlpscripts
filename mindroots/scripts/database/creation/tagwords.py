from neo4j import GraphDatabase

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


def check_word_type(tx):
    query = """
    MATCH (w:Word)
    WHERE w.arabic IS NOT NULL
    RETURN w.arabic AS word
    LIMIT 50
    """
    result = tx.run(query)
    
    for record in result:
        word = record["word"]
        words_list = word.split()  # Split the Arabic string into words

        if len(words_list) > 2:
            # Mark as a phrase if there are more than two words
            print(f"Word: {word}, Classification: Phrase (More than two words)")
        else:
            last_char = words_list[-1][-1]  # Check the last character of the last word

            if last_char == '\u064E':  # Fatha
                print(f"Word: {word}, Classification: Verb")
            else:
                print(f"Word: {word}, Classification: Noun")

# Run the function
with driver.session() as session:
    session.execute_read(check_word_type)

driver.close()