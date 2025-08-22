import os
import csv
import json
import time
import tiktoken
import glob
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
def read_csv_hardcoded(files, limit_definition_length=300):
    all_words_data = []
    for file_path in files:
        print(f"Reading file: {file_path}")
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                word = row['word']
                definition = row['definitions_xml']

                # Limit the length of the definition
                if len(definition) > limit_definition_length:
                    definition = definition[:limit_definition_length] + "..."

                all_words_data.append((word, definition))
    return all_words_data

# Step 2: Create batch input file for OpenAI Batch API
def create_batch_input_file_by_tokens(words_data, file_name_prefix, max_tokens=70000, model="gpt-4o"):
    encoding = tiktoken.encoding_for_model(model)
    batch_index = 1
    start_idx = 0

    while start_idx < len(words_data):
        batch_tokens = 0
        batch_data = []

        for idx, (word, definition) in enumerate(words_data[start_idx:], start=start_idx):
            messages = [
                {
                    "role": "system",
                    "content": (
                        "You are helping me update a large graph database of Arabic words. "
                        "Be concise and do not exceed more than 1 or 2 words for each case unless it's a short phrase. "
                        "Return only the English translation, the Spanish translation, the Urdu translation and the Roman transliteration, "
                        "in the following JSON format (no extra text):\n\n"
                        "{\n"
                        "  \"english\": \"\",\n"
                        "  \"spanish\": \"\",\n"
                        "  \"urdu\": \"\",\n"
                        "  \"transliteration\": \"\"\n"
                        "}"
                    ),
                },
                {
                    "role": "user",
                    "content": f"Translate the following Arabic word or phrase: '{word}' and use this accompanying dictionary definition from Lane's Lexicon for context: {definition}",
                }
            ]

            # Count tokens for this message
            message_tokens = sum(len(encoding.encode(msg["content"])) for msg in messages)

            # Stop adding if this request would exceed the token limit
            if batch_tokens + message_tokens > max_tokens:
                break

            batch_tokens += message_tokens
            batch_data.append({
                "custom_id": f"request-{idx}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": model,
                    "messages": messages
                }
            })

        # Write the batch to a file
        batch_file_name = f"{file_name_prefix}_batch_{batch_index}.jsonl"
        with open(batch_file_name, 'w', encoding='utf-8') as f:
            for request in batch_data:
                f.write(json.dumps(request) + "\n")
        print(f"Batch file '{batch_file_name}' created with {len(batch_data)} entries and {batch_tokens} tokens.")

        # Update indices for next batch
        start_idx += len(batch_data)
        batch_index += 1

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
        time.sleep(300)  # Wait for 1 minute before checking again
    print(f"Batch {batch_id} completed.")
    return batch.output_file_id

# Step 6: Retrieve the results of the batch
def retrieve_batch_results(output_file_id, output_file_name="batched_output.jsonl"):
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
    print("2. Sequentially create, upload, poll, and process batches (send requests to OpenAI)")

    choice = input("Enter the number of the option you want to execute: ")

    # List of hardcoded chunk files
    chunk_files = [
        "lane_chunk_1.csv",
        "lane_chunk_2.csv",
        "lane_chunk_3.csv",
        "lane_chunk_4.csv",
        "lane_chunk_5.csv",
    ]

    if choice == "1":
        # Process existing batch results
        words_data = read_csv_hardcoded(chunk_files, limit_definition_length=200)
        process_existing_batch_results(words_data)
    elif choice == "2":
        # Step 1: Create all batch input files
        max_tokens = 2000000  # Token limit for each batch
        batch_file_list = []  # To store created batch files

        for chunk_file in chunk_files:
            print(f"Processing chunk: {chunk_file}")
            words_data = read_csv_hardcoded([chunk_file], limit_definition_length=250)

            # Create batches for the current chunk
            create_batch_input_file_by_tokens(
                words_data,
                file_name_prefix=f"chunk_{chunk_file.split('_')[-1].split('.')[0]}",
                max_tokens=max_tokens,
                model="gpt-4o-mini"
            )

            # Collect all batch files for uploading
            chunk_batch_files = sorted(glob.glob(f"chunk_{chunk_file.split('_')[-1].split('.')[0]}_batch_*.jsonl"))
            batch_file_list.extend(chunk_batch_files)

        # Step 2: Sequentially upload, poll, and process batches
        print(f"Sequentially processing {len(batch_file_list)} batch files...")
        for batch_file in batch_file_list:
            print(f"Uploading batch file: {batch_file}")
            batch_input_file_id = upload_batch_file(file_name=batch_file)
            batch_id = create_batch(batch_input_file_id)
            print(f"Batch ID {batch_id} created for file: {batch_file}")

            # Poll until the batch is completed
            print(f"Polling for batch ID {batch_id}...")
            output_file_id = poll_batch_status(batch_id)

            # Retrieve and process the results
            output_file_name = f"batch_output_for_{batch_file}.jsonl"
            retrieve_batch_results(output_file_id, output_file_name=output_file_name)
            process_batch_results(output_file_name=output_file_name, words_data=words_data)

        print("All batches created, uploaded, and processed successfully.")
    else:
        print("Invalid choice. Please run the script again.")

if __name__ == "__main__":
    main()