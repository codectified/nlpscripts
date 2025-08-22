import os
import csv
import json
import time
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the OpenAI API key from the environment
api_key = os.getenv('OPENAI_API_KEY')

if not api_key:
    raise ValueError("OpenAI API key is missing. Set it in your .env file or environment variables.")

# Instantiate the OpenAI client with the API key
client = OpenAI(api_key=api_key)

# Step 1: Read the CSV file and fetch word and definitions
def read_csv(file_path, limit_definition_length=500):
    words_data = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            word = row['word']
            definition = row['definitions_xml']

            # Limit the length of the definition
            if len(definition) > limit_definition_length:
                definition = definition[:limit_definition_length] + "..."

            words_data.append((word, definition))
    return words_data

# Step 2: Create batch input file for OpenAI Batch API
def create_batch_input_file(words_data, file_name="batchinput.jsonl", start_idx=0, batch_size=1000):
    with open(file_name, 'w', encoding='utf-8') as f:
        for idx, (word, definition) in enumerate(words_data[start_idx:start_idx + batch_size]):
            batch_request = {
                "custom_id": f"request-{start_idx + idx}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": "You are a helpful assistant trained to identify the grammatical nature of Arabic words. Your task is to classify the word or phrase into one of 9 categories:  المصدر، اسم الفاعل، اسم المفعول، اسم المكان، اسم الحال، اسم الآلة، اسم ذات، اسم المبالغة، اسم العلة  Please limnit your response to only one of the aforementioned categories. If there is not enough information then respond with 'NA'."},
                        {"role": "user", "content": f"Classify the following Arabic word or phrase: '{word}' as one of the following:  المصدر، اسم الفاعل، اسم المفعول، اسم المكان، اسم الحال، اسم الآلة، اسم ذات، اسم المبالغة، اسم العلة. If necessary, use this accompanying dictionary definition from Lane's Lexicon for more context: {definition}"
}
                    ]
                }
            }
            f.write(json.dumps(batch_request) + "\n")
    print(f"Batch input file '{file_name}' created with {len(words_data[start_idx:start_idx + batch_size])} entries.")


# Step 3: Upload the batch file to OpenAI
def upload_batch_file(file_name="batchinput.jsonl"):
    with open(file_name, 'rb') as f:
        batch_input_file = client.files.create(file=f, purpose="batch")
    print(f"File '{file_name}' uploaded with ID {batch_input_file.id}")
    return batch_input_file.id

# Step 4: Create the batch and process requests
def create_batch(batch_input_file_id):
    batch = client.batches.create(
        input_file_id=batch_input_file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"description": "Processing Arabic words batch"}
    )
    print(f"Batch created with ID {batch.id}.")
    return batch.id

# Step 5: Poll for batch completion status
def poll_batch_status(batch_id):
    status = None
    while status != "completed":
        batch = client.batches.retrieve(batch_id)
        status = batch.status
        print(f"Batch {batch_id} is currently {status}.")
        if status == "failed" or status == "cancelled":
            raise Exception(f"Batch {batch_id} failed or was cancelled.")
        time.sleep(60)  # Wait for 1 minute before checking again
    print(f"Batch {batch_id} completed.")
    return batch.output_file_id

# Step 6: Retrieve the results of the batch
def retrieve_batch_results(output_file_id, output_file_name="batch_output.jsonl"):
    result = client.files.content(output_file_id)
    with open(output_file_name, 'w', encoding='utf-8') as f:
        f.write(result.text)
    print(f"Batch results saved to '{output_file_name}'.")

# Step 7: Optional - Update a database or process results (this step is simplified for now)
# Step 7: Update to show the initial Arabic word with the summary
def process_batch_results(output_file_name="batch_output.jsonl", words_data=None):
    with open(output_file_name, 'r', encoding='utf-8') as f:
        for line in f:
            result = json.loads(line)
            custom_id = result['custom_id']
            summary = result['response']['body']['choices'][0]['message']['content'].strip()

            # The custom_id is "request-{idx}", so we'll split by "-" and get the index
            request_index = int(custom_id.split("-")[1])

            # Retrieve the corresponding Arabic word and definition from words_data using the index
            arabic_word, _ = words_data[request_index]

            # Print the Arabic word alongside the summary
            print(f"Arabic Word: {arabic_word} | Summary: {summary}")

def process_existing_batch_results(words_data):
    process_batch_results(output_file_name="batch_output.jsonl", words_data=words_data)

def main():
    print("Choose an option:")
    print("1. Process existing batch results")
    print("2. Run full pipeline (send requests to OpenAI)")

    choice = input("Enter the number of the option you want to execute: ")

    if choice == "1":
        # Process existing batch results
        words_data = read_csv("clean_defs.csv", limit_definition_length=500)
        process_existing_batch_results(words_data)
    elif choice == "2":
        # Run the full pipeline
        words_data = read_csv("clean_defs.csv", limit_definition_length=500)
        total_words = len(words_data)
        batch_size = 1000 # Adjust batch size as needed

        for start_idx in range(0, total_words, batch_size):
            batch_file_name = f"batchinput_{start_idx//batch_size + 1}.jsonl"
            create_batch_input_file(words_data, file_name=batch_file_name, start_idx=start_idx, batch_size=batch_size)
            batch_input_file_id = upload_batch_file(file_name=batch_file_name)
            batch_id = create_batch(batch_input_file_id)
            output_file_id = poll_batch_status(batch_id)
            retrieve_batch_results(output_file_id, output_file_name=f"batch_output_{start_idx//batch_size + 1}.jsonl")
            process_batch_results(output_file_name=f"batch_output_{start_idx//batch_size + 1}.jsonl", words_data=words_data)
            
    else:
        print("Invalid choice. Please run the script again.")

if __name__ == "__main__":
    main()