from deepdiff import DeepDiff
import json
import pprint

def read_json_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def compare_objects(obj1, obj2):
    return DeepDiff(obj1, obj2, ignore_order=True)

def compare_json_files(file1_path, file2_path):
    json_content1 = read_json_file(file1_path)
    json_content2 = read_json_file(file2_path)

    # Creating lookup tables using the 'id' field as the key
    lookup_table1 = {item['id']: item for item in json_content1}
    lookup_table2 = {item['id']: item for item in json_content2}

    # Dictionary to store the differences
    differences_by_id = {}

    # Iterating through the first JSON file, finding the corresponding object in the second JSON file, and comparing them
    for obj_id, obj1 in lookup_table1.items():
        obj2 = lookup_table2.get(obj_id)
        if obj2:
            differences = compare_objects(obj1, obj2)
            if differences:
                differences_by_id[obj_id] = differences

    return differences_by_id

file1_path = '/Users/omaribrahim/dev/nyl/dev.b-datasources' # Replace with the correct path
file2_path = '/Users/omaribrahim/dev/nyl/qa.b-datasources' # Replace with the correct path

differences = compare_json_files(file1_path, file2_path)

# You can now print or further process the differences
# Pretty-printing the differences
pp = pprint.PrettyPrinter(indent=4)
for obj_id, diff in differences.items():
    print(f"Differences for ID {obj_id}:")
    pp.pprint(diff)
