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

# Step 1: Read the CSV file and fetch Arabic words
def read_csv(file_path):
    words_data = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            word = row['word']
            words_data.append(word)
    return words_data

# Step 2: Create batch input file for OpenAI Batch API
def create_batch_input_file(words_data, file_name="batchinput.jsonl", start_idx=0, batch_size=1000):
    with open(file_name, 'w', encoding='utf-8') as f:
        for idx, word in enumerate(words_data[start_idx:start_idx + batch_size]):
            batch_request = {
                "custom_id": f"request-{start_idx + idx}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": "You are a helpful assistant trained to identify the grammatical nature of Arabic words. Your task is to classify the word or phrase into one of three categories.  Please limnit your response to only the a single word: noun, verb, phrase or letter.  If there is not enough information then respond with 'NA'."},
                        {"role": "user", "content": f"Please classify the following Arabic word or phrase: '{word}' as either 'noun', 'verb', or 'phrase'."}
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

# Step 7: Process the results and classify the words
def process_batch_results(output_file_name="batch_output.jsonl"):
    """
    Read each JSONL line from the batch output, parse the JSON string returned
    by the model, and return a list of tuples:
      (entry_id, wazn, form)
    """
    results = []
    with open(output_file_name, 'r', encoding='utf-8') as f:
        for line in f:
            record = json.loads(line)
            try:
                content_json = record["response"]["body"]["choices"][0]["message"]["content"].strip()
                parsed = json.loads(content_json)
                entry_id = parsed["id"]
                wazn     = parsed["wazn"]
                form     = parsed["form"]
                results.append((entry_id, wazn, form))
                print(f"{entry_id:10} | {wazn:20} | {form}")
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Skipping malformed line: {e}")
    return results




def process_existing_batch_results(words_data):
    process_batch_results(output_file_name="batch_output.jsonl", words_data=words_data)



def main():
    print("Choose an option:")
    print("1. Process existing batch results")
    print("2. Run full pipeline (send requests to OpenAI)")

    choice = input("Enter the number of the option you want to execute: ")

    if choice == "1":
        # Process existing batch results
        words_data = read_csv("clean_defs.csv")
        process_existing_batch_results(words_data)
    elif choice == "2":
        # Run the full pipeline
        words_data = read_csv("clean_defs.csv")
        total_words = len(words_data)
        
        # Set the batch size for testing
        batch_size = 5000  # Change this value to the number of rows you want to process (e.g., 50 or 100)
        
        # Test with only the first few batches
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