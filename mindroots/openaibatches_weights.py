import os
import csv
import json
import time
from openai import OpenAI
from dotenv import load_dotenv

# ─── Environment & Client Setup ────────────────────────────────────────────────
# Load API credentials and initialize the OpenAI client. This keeps secrets
# out of source control and centralizes configuration.
load_dotenv()
api_key = os.getenv('OPENAI_API_KEY')
if not api_key:
    raise ValueError("OpenAI API key is missing. Set it in your .env file or environment variables.")
client = OpenAI(api_key=api_key)

def get_batch_indexes(directory=".", input_prefix="batchinput_", output_prefix="batch_output_", suffix=".jsonl"):
    input_batches = set()
    output_batches = set()

    for filename in os.listdir(directory):
        if filename.startswith(input_prefix) and filename.endswith(suffix):
            try:
                batch_num = int(filename[len(input_prefix):-len(suffix)])
                input_batches.add(batch_num)
            except ValueError:
                continue
        elif filename.startswith(output_prefix) and filename.endswith(suffix):
            try:
                batch_num = int(filename[len(output_prefix):-len(suffix)])
                output_batches.add(batch_num)
            except ValueError:
                continue
    return input_batches, output_batches


# ─── Step 1: Ingest Word Data ────────────────────────────────────────────────────
def read_csv(file_path, limit_definition_length=500):
    """
    Read raw word entries from a CSV and truncate definitions.
    Returns a list of tuples: (entry_id, arabic_word, truncated_definition).
    """
    words_data = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            entry_id  = row['entry_id_xml']       # carry your real node ID
            word      = row['word']
            definition = row['definitions_xml']
            if len(definition) > limit_definition_length:
                definition = definition[:limit_definition_length] + "..."
            words_data.append((entry_id, word, definition))
    return words_data

# ─── Step 2: Prepare LLM Batch Requests ─────────────────────────────────────────
import json

def create_batch_input_file(words_data, file_name="batchinput.jsonl", start_idx=0, batch_size=1000):
    system_prompt = (
        "أنت خبير في الصرف العربي. أعطَ الكلمة المعطاة فقط الوزن الصرفي القياسي الكامل بالتشكيل، "
        "مع تمثيله أيضًا في سطر واحد باستخدام الرموز:\n"
        " - استخدم الرقم 3 لحرف ع\n"
        " - استخدم 33 لتكرار ع مع الشدة\n\n"
        "يجب أن تُرجع فقط كائن JSON بهذا الشكل:\n"
        "{\n"
        "  \"id\": \"<نفس المعرف>\",\n"
        "  \"wazn\": \"<وزن كامل بالتشكيل>\",\n"
        "  \"form\": \"<transliteration>\"\n"
        "}\n\n"
        "إذا لم يوجد وزن معروف، استخدم:\n"
        "{\"id\": \"<نفس المعرف>\", \"wazn\": \"NA\", \"form\": \"NA\"}\n\n"
        "**أمثلة واضحة:**\n"
        "{\"id\":\"001\",\"wazn\":\"فَعَلَ\",\"form\":\"fa3ala\"} ← كَتَبَ\n"
        "{\"id\":\"002\",\"wazn\":\"فَعَّلَ\",\"form\":\"fa33ala\"} ← عَلَّمَ\n"
        "{\"id\":\"003\",\"wazn\":\"أَفْعَلَ\",\"form\":\"af3ala\"} ← أَكْرَمَ\n"
        "{\"id\":\"004\",\"wazn\":\"فَاعَلَ\",\"form\":\"fā3ala\"} ← صَاحَبَ\n"
        "{\"id\":\"005\",\"wazn\":\"مُفْعَلٌ\",\"form\":\"muf3alun\"} ← مُجْتَمَعٌ\n"
        "{\"id\":\"006\",\"wazn\":\"فَعَّال\",\"form\":\"fa33āl\"} ← فَتَّاح\n"
        "{\"id\":\"007\",\"wazn\":\"مِفْعَال\",\"form\":\"mif3āl\"} ← مِقْدَام\n"
        "{\"id\":\"008\",\"wazn\":\"فَعُول\",\"form\":\"fa3ūl\"} ← شَكُور\n"
        "{\"id\":\"009\",\"wazn\":\"فَعيل\",\"form\":\"fa3īl\"} ← عَلِيم\n"
        "{\"id\":\"010\",\"wazn\":\"فَعِل\",\"form\":\"fa3il\"} ← حَذِر\n"
        "{\"id\":\"011\",\"wazn\":\"فِعَالٌ\",\"form\":\"fi3āl\"} ← كِتَابٌ\n"
        "{\"id\":\"012\",\"wazn\":\"فُعُلٌ\",\"form\":\"fu3ul\"} ← كُتُبٌ\n"
        "{\"id\":\"013\",\"wazn\":\"فُعَّال\",\"form\":\"fu33āl\"} ← كُتَّاب\n"
        "{\"id\":\"014\",\"wazn\":\"فَعيلٌ\",\"form\":\"fa3īlun\"} ← سَمِيعٌ\n"
        "{\"id\":\"015\",\"wazn\":\"فَعُولٌ\",\"form\":\"fa3ūlun\"} ← رَسُولٌ\n"
        "{\"id\":\"016\",\"wazn\":\"فَعْلَةٌ\",\"form\":\"fa3lah\"} ← جَلْسَةٌ\n"
        "{\"id\":\"017\",\"wazn\":\"مَفْعُولٌ\",\"form\":\"maf3ūlun\"} ← مَقْبُولٌ\n"
        "{\"id\":\"018\",\"wazn\":\"مِفْعَلٌ\",\"form\":\"mif3al\"} ← مِفْتَاحٌ\n"
        "{\"id\":\"019\",\"wazn\":\"فَعَّالَةٌ\",\"form\":\"fa33ālah\"} ← طَبَّاخَةٌ\n\n"
        "لا تُرجِع أي شرح. فقط كائن JSON."
    )

    with open(file_name, 'w', encoding='utf-8') as f:
        for entry_id, word, definition in words_data[start_idx:start_idx + batch_size]:
            batch_request = {
                "custom_id": entry_id,
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": f'ID: "{entry_id}"\nWord: "{word}"'}
                    ]
                }
            }
            f.write(json.dumps(batch_request, ensure_ascii=False) + "\n")

    print(f"✅ Batch input file '{file_name}' created with {min(batch_size, len(words_data) - start_idx)} entries.")

# ─── Step 3: Push Batch File to OpenAI ───────────────────────────────────────────
def upload_batch_file(file_name="batchinput.jsonl"):
    """
    Upload the prepared JSONL as an OpenAI File resource for batch processing.
    Abstractly: stage LLM payloads on the server → returns file handle.
    """
    with open(file_name, 'rb') as f:
        batch_input_file = client.files.create(file=f, purpose="batch")
    print(f"File '{file_name}' uploaded with ID {batch_input_file.id}")
    return batch_input_file.id


# ─── Step 4: Kick Off the Batch Job ─────────────────────────────────────────────
def create_batch(batch_input_file_id):
    """
    Instruct OpenAI to execute the batch of chat completions.
    Abstractly: schedule asynchronous LLM work → returns batch job handle.
    """
    batch = client.batches.create(
        input_file_id=batch_input_file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"description": "Processing Arabic words batch"}
    )
    print(f"Batch created with ID {batch.id}.")
    return batch.id


# ─── Step 5: Poll for Completion ─────────────────────────────────────────────────
def poll_batch_status(batch_id):
    """
    Periodically check the status of the batch job until it finishes.
    Abstractly: wait for LLM work to complete → handle failures → return output handle.
    """
    status = None
    while status != "completed":
        batch = client.batches.retrieve(batch_id)
        status = batch.status
        print(f"Batch {batch_id} status: {status}")
        if status in ("failed", "cancelled"):
            raise Exception(f"Batch {batch_id} did not complete: {status}")
        time.sleep(60)  # back off between polls
    print(f"Batch {batch_id} completed.")
    return batch.output_file_id


# ─── Step 6: Fetch Results ────────────────────────────────────────────────────────
def retrieve_batch_results(output_file_id, output_file_name="batch_output.jsonl"):
    """
    Download the completed batch’s results as JSONL.
    Abstractly: pull LLM responses back to local storage for post-processing.
    """
    result = client.files.content(output_file_id)
    with open(output_file_name, 'w', encoding='utf-8') as f:
        f.write(result.text)
    print(f"Results saved to '{output_file_name}'.")


# ─── Step 7: Post‐Process & Integrate ─────────────────────────────────────────────
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

def main():
    print("Options:\n1. Process existing results\n2. Run full pipeline (resumable)")
    choice = input("Select 1 or 2: ").strip()

    words_data = read_csv("clean_defs.csv", limit_definition_length=500)
    batch_size = 1000
    total = len(words_data)

    input_batches, output_batches = get_batch_indexes()

    if choice == "1":
        # This can loop through all batch_output files if needed
        for batch_num in sorted(output_batches):
            output_name = f"batch_output_{batch_num}.jsonl"
            process_batch_results(output_file_name=output_name)

    elif choice == "2":
        for start in range(0, total, batch_size):
            batch_index = (start // batch_size) + 1
            input_file = f"batchinput_{batch_index}.jsonl"
            output_file = f"batch_output_{batch_index}.jsonl"

            if batch_index in output_batches:
                print(f"✔️ Skipping batch {batch_index} (already retrieved)")
                continue

            # If the input file doesn't exist, create it
            if batch_index not in input_batches:
                print(f"📝 Creating new batch input file {input_file}")
                create_batch_input_file(words_data, file_name=input_file, start_idx=start, batch_size=batch_size)
            else:
                print(f"↪️ Reusing existing input file {input_file}")

            # Send + retrieve output
            file_id = upload_batch_file(file_name=input_file)
            batch_id = create_batch(file_id)
            out_id = poll_batch_status(batch_id)
            retrieve_batch_results(out_id, output_file_name=output_file)
            process_batch_results(output_file_name=output_file)

    else:
        print("Invalid choice. Exiting.")
        
if __name__ == "__main__":
    main()