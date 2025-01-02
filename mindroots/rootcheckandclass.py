from py2neo import Graph
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Get the Neo4j connection details from the environment
uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASS')

class RootUpdater:

    def __init__(self, uri, user, password):
        # Connect to the Neo4j database
        self.graph = Graph(uri, auth=(user, password))
        self.type_counters = {}

    def initialize_counters(self):
        print("Initializing counters for root types...")
        root_types = ["Geminate", "Triliteral", "Quadriliteral", "Quintiliteral", "Hexaliteral", "Heptaliteral", "BeyondSeptiliteral"]
        for root_type in root_types:
            # Query the database for the highest existing ID for each root type
            result = self.graph.run(
                f"""
                MATCH (r:Root)
                WHERE r.root_type = $root_type AND EXISTS(r.{root_type}_ID)
                RETURN MAX(r.{root_type}_ID) AS max_id
                """,
                root_type=root_type
            ).evaluate()
            self.type_counters[root_type] = result + 1 if result else 1
        print(f"Type counters initialized: {self.type_counters}\n")

    def update_roots(self):
        print("Fetching root nodes from the database...")
        roots = self.graph.run("MATCH (r:Root) RETURN r.root_id AS id, r.arabic AS arabic").data()
        print(f"Found {len(roots)} root nodes to process.\n")

        for record in roots:
            root_id = record['id']
            arabic = record['arabic']
            
            # Split into letters and reverse for RTL order
            letters = arabic.split('-')[::-1]  # Reverse for RTL
            unique_letters = list(dict.fromkeys(letters))  # Remove duplicates for classification
            
            # Assign r1, r2, r3, ... based on RTL order
            r_properties = {f"r{i+1}": letter for i, letter in enumerate(letters)}
            
            # Determine the root type and its corresponding ID
            root_type, type_id_field = self.get_root_type_and_field(unique_letters)

            # Increment the counter for this root type
            type_id = self.type_counters[root_type]
            self.type_counters[root_type] += 1

            # Update the root node in the database
            self.graph.run(
                f"""
                MATCH (r:Root {{root_id: $root_id}})
                SET r += $properties,
                    r.root_type = $root_type,
                    r.{type_id_field} = $type_id
                """,
                root_id=root_id,
                properties=r_properties,
                root_type=root_type,
                type_id=type_id
            )

            # Log the updated properties
            updated_properties = {
                "root_id": root_id,
                "arabic": arabic,
                "r_values": r_properties,
                "root_type": root_type,
                type_id_field: type_id
            }
            print(f"Updated Root {root_id}: {updated_properties}")

        print("\nProcessing completed!")

    def get_root_type_and_field(self, unique_letters):
        count = len(unique_letters)
        if count == 2:  # Geminate
            return 'Geminate', 'Geminate_ID'
        elif count == 3:  # Triliteral
            return 'Triliteral', 'Triliteral_ID'
        elif count == 4:  # Quadriliteral
            return 'Quadriliteral', 'Quadriliteral_ID'
        elif count == 5:  # Quintiliteral
            return 'Quintiliteral', 'Quintiliteral_ID'
        elif count == 6:  # Hexaliteral
            return 'Hexaliteral', 'Hexaliteral_ID'
        elif count == 7:  # Heptaliteral
            return 'Heptaliteral', 'Heptaliteral_ID'
        else:  # Beyond Septiliteral
            return 'BeyondSeptiliteral', 'BeyondSeptiliteral_ID'

    def print_summary(self):
        print("\n--- Summary Report ---")
        print(f"Type counters after processing: {self.type_counters}")
        print("----------------------\n")


# Usage
if __name__ == "__main__":
    if not uri or not user or not password:
        raise ValueError("Neo4j connection details are not set in environment variables.")

    updater = RootUpdater(uri, user, password)
    updater.initialize_counters()
    updater.update_roots()
    updater.print_summary()
    print("Root nodes have been successfully updated.")