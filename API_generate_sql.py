import os
import json
import glob
import sqlite3
import argparse
import time
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

def log(msg: str):
    """Prints a message with a timestamp."""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}")

# Initialize the OpenAI client.
# It's recommended to set the API key via environment variables for security.
client = OpenAI(
    base_url=" ",  # Replace with your API base URL if using a proxy
    api_key=os.environ.get("OPENAI_API_KEY", "YOUR_API_KEY") # Reads from env var or uses placeholder
)

def get_tables_and_columns(sqlite_path: str) -> dict:
    """
    Connects to a SQLite database and extracts table and column names.
    """
    tables_columns = {}
    try:
        with sqlite3.connect(sqlite_path) as conn:
            cursor = conn.cursor()
            # Query for all table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
            tables = cursor.fetchall()
            for (table_name,) in tables:
                # For each table, get its column names
                cursor.execute(f"PRAGMA table_info('{table_name}')")
                columns_info = cursor.fetchall()
                column_names = [col_info[1] for col_info in columns_info]
                tables_columns[table_name] = column_names
    except sqlite3.Error as e:
        log(f"Error processing SQLite file {sqlite_path}: {e}")
    return tables_columns

def format_table_info(tables_columns: dict) -> str:
    """
    Formats the schema information into a single string.
    Example: "table1: col1, col2; table2: colA, colB"
    """
    table_info_list = []
    for table, columns in tables_columns.items():
        table_info = f"{table}: " + ", ".join(columns)
        table_info_list.append(table_info)
    return "; ".join(table_info_list)

def construct_prompt(question: str, schema: str, sql_engine: str) -> str:
    """
    Constructs the prompt for the LLM with the specified SQL engine.
    """
    prompt = (
        f"You need to generate a {sql_engine} query based on the following question and the detailed table and column information of the database provided below. "
        f"The output should only be the {sql_engine} query in one line, without explanation or comments.\n\n"
        f"### Question:\n{question}\n\n"
        f"### Table and column information:\n{schema}\n\n"
        f"### {sql_engine} Query:"
    )
    return prompt

def send_api_request(prompt: str, model_name: str, index: str, db_id: str, attempt: int = 1, retry_limit: int = 3) -> tuple:
    """
    Sends a request to the OpenAI API with a retry mechanism.
    """
    try:
        chat_completion = client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model=model_name,
            temperature=0
        )
        result = chat_completion.choices[0].message.content.strip().replace("\n", " ")
        return index, result, db_id
    except Exception as e:
        log(f"API call failed (index={index}, attempt {attempt}/{retry_limit}): {e}")
        if attempt < retry_limit:
            time.sleep(attempt) # Exponential backoff
            return send_api_request(prompt, model_name, index, db_id, attempt + 1, retry_limit)
        return index, f"Error: API request failed after {retry_limit} attempts.", db_id

def main():
    parser = argparse.ArgumentParser(description="Generate SQL queries using a closed-source model.")
    parser.add_argument('--data_file', type=str, required=True, help="Path to the input JSON file (e.g., test.json).")
    parser.add_argument('--output_file', type=str, required=True, help="Path for the output file (format: index<TAB>SQL<TAB>db_id).")
    parser.add_argument('--closed_model', type=str, required=True, help="Name of the closed-source model (e.g., gpt-4o).")
    parser.add_argument('--db_dir', type=str, required=True, help="Root directory for databases (e.g., spider_data/test_database).")
    parser.add_argument('--sql_engine', type=str, default='MySQL', choices=['MySQL', 'SQLite', 'PostgreSQL', 'TSQL'], help="The SQL dialect to generate (default: MySQL).")
    parser.add_argument('--max_workers', type=int, default=5, help="Maximum number of concurrent workers (default: 5).")
    args = parser.parse_args()

    log(f"Loading data from {args.data_file}...")
    with open(args.data_file, "r", encoding="utf-8") as f:
        data_items = json.load(f)

    tasks = []
    for item in data_items:
        index = item.get("index") or item.get("Index")
        db_id = item.get("db_id")
        question = item.get("question")

        if not all([index is not None, db_id, question]):
            log(f"Skipping item due to missing 'index', 'db_id', or 'question': {item}")
            continue

        db_path_dir = os.path.join(args.db_dir, db_id)
        if not os.path.exists(db_path_dir):
            log(f"❌ Directory {db_path_dir} does not exist. Skipping db_id: {db_id}")
            continue

        sqlite_file_path = os.path.join(db_path_dir, f"{db_id}.sqlite")
        if not os.path.exists(sqlite_file_path):
            log(f"❌ No .sqlite file found at {sqlite_file_path}. Skipping db_id: {db_id}")
            continue
        
        schema_info = get_tables_and_columns(sqlite_file_path)
        if not schema_info:
            log(f"❌ Could not extract schema from {sqlite_file_path}. Skipping db_id: {db_id}")
            continue

        schema_str = format_table_info(schema_info)
        prompt = construct_prompt(question, schema_str, args.sql_engine)
        tasks.append((str(index), prompt, db_id))

    results = []
    log(f"Submitting {len(tasks)} tasks to the API using {args.max_workers} workers...")
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        future_to_task = {executor.submit(send_api_request, prompt, args.closed_model, index, db_id): (index, db_id) for index, prompt, db_id in tasks}

        for future in as_completed(future_to_task):
            index, db_id = future_to_task[future]
            try:
                res_index, sql_text, res_db_id = future.result()
                log(f"✅ Completed task index={res_index}, db_id={res_db_id}")
                results.append((int(res_index), sql_text, res_db_id))
            except Exception as e:
                log(f"❌ Task failed for index={index}, db_id={db_id}: {e}")
                results.append((int(index), f"Error: Task execution failed.", db_id))

    # Sort results by index to ensure consistent output order
    results.sort(key=lambda x: x[0])
    
    log(f"Writing {len(results)} results to {args.output_file}...")
    with open(args.output_file, "w", encoding="utf-8") as f:
        for index, sql_text, db_id in results:
            f.write(f"{index}\t{sql_text}\t{db_id}\n")

    log(f"✅ Generation complete. Results saved to {args.output_file}")

if __name__ == "__main__":
    main()
