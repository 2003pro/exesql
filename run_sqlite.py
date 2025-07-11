import os
import json
import sqlite3
import multiprocessing
from tqdm import tqdm

# Timeout in seconds for each SQL execution
TIMEOUT_SECONDS = 30

def execute_sql(sql, db_path, return_dict):
    """
    Executes an SQL query directly on a SQLite database file.
    Replaces LIKE with GLOB for case-sensitive matching.
    """
    try:
        # 1. Connect directly to the target SQLite database file
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 2. Set PRAGMA for case-sensitive LIKE matching
        cursor.execute("PRAGMA case_sensitive_like = true;")

        # 3. Replace LIKE with GLOB and % with * for compatibility
        if "LIKE" in sql:
            sql = sql.replace("LIKE", "GLOB").replace("%", "*")

        # 4. Execute the SQL query
        cursor.execute(sql)
        result = cursor.fetchall()
        return_dict["result"] = result

        conn.close()
    except Exception as e:
        return_dict["error"] = str(e)

def run_sql_and_save_results(gold_file, db_dir, output_file):
    """
    Reads SQL queries from a JSON file, executes them on corresponding SQLite databases,
    and writes the results to an output file.
    """
    results = []

    # Load JSON input
    with open(gold_file, 'r', encoding='utf-8') as f:
        entries = json.load(f)

    for entry in tqdm(entries, desc="Processing"):
        # Extract fields from each JSON entry
        index = entry.get('index')
        sql = entry.get('SQL')
        db_id = entry.get('db_id')

        if index is None or sql is None or db_id is None:
            results.append(f"{index}\tError: Missing one of index/query/db_id in JSON entry.\n")
            continue

        db_path = os.path.join(db_dir, db_id, f"{db_id}.sqlite")
        if not os.path.exists(db_path):
            results.append(f"{index}\tError: Database file {db_path} not found.\n")
            continue

        # Use a separate process to run the SQL query with timeout protection
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        p = multiprocessing.Process(target=execute_sql, args=(sql, db_path, return_dict))
        p.start()
        p.join(TIMEOUT_SECONDS)

        if p.is_alive():
            p.terminate()
            p.join()
            results.append(f"{index}\tError: Timeout after {TIMEOUT_SECONDS} seconds.\n")
        else:
            if "error" in return_dict:
                results.append(f"{index}\tError: {return_dict['error']}\n")
            else:
                results.append(f"{index}\t{return_dict['result']}\n")

    # Save all results to output file
    with open(output_file, 'w', encoding='utf-8') as output:
        output.writelines(results)

    print(f"[✓] Results saved to {output_file}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run SQL queries from a JSON file on SQLite databases and save the results.")
    parser.add_argument('--gold', type=str, required=True,
                        help="Path to the JSON file (e.g., spider_data/train_spider_index1.json)")
    parser.add_argument('--db_dir', type=str, required=True,
                        help="Path to the directory containing subfolders with SQLite .sqlite files.")
    parser.add_argument('--output', type=str, default='gold_result.txt',
                        help="Path to the output file where results will be written.")

    args = parser.parse_args()

    run_sql_and_save_results(
        gold_file=args.gold,
        db_dir=args.db_dir,
        output_file=args.output
    )
