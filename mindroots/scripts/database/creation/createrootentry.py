from py2neo import Graph
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Get the Neo4j connection details from the environment
uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASS')

class EntryUpdater:
    def __init__(self, uri, user, password):
        # Connect to the Neo4j database
        self.graph = Graph(uri, auth=(user, password))

    def update_entry_from_file(self, root_id, file_name):
        """
        Updates the 'entry' property for a specified root node by reading from a text file.
        Looks for the file in the same directory as this script.
        """
        # Get the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, file_name)

        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                entry_text = file.read()
                self.graph.run(
                    """
                    MATCH (r:Root {root_id: $root_id})
                    SET r.entry = $entry_text
                    RETURN r
                    """,
                    root_id=root_id,
                    entry_text=entry_text
                )
            print(f"Updated entry property for Root node with root_id {root_id}.")
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    if not uri or not user or not password:
        raise ValueError("Neo4j connection details are not set in environment variables.")

    updater = EntryUpdater(uri, user, password)
    
    # Prompt for user input
    root_id = int(input("Enter the root_id of the node to update: "))
    file_name = input("Enter the name of the text file containing the new entry: ")

    # Update the entry property from the specified file
    updater.update_entry_from_file(root_id, file_name)