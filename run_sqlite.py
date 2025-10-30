import os
import sqlite3
import time
import argparse
import multiprocessing as mp
from tqdm import tqdm

def execute_single_sql(args_tuple):
    """
    Worker function to execute a single SQL query on its corresponding database.
    This function is designed to be called by a multiprocessing pool.

    Args:
        args_tuple (tuple): A tuple containing index, sql, db_id, and db_dir.

    Returns:
        dict: A dictionary containing the result, including index, db_id,
              status, data (result or error message), and execution duration.
    """
    index, sql, db_id, db_dir = args_tuple
    db_path = os.path.join(db_dir, db_id, f"{db_id}.sqlite")

    if not os.path.exists(db_path):
        return {
            "index": index,
            "db_id": db_id,
            "status": "error",
            "data": f"Database file {db_path} not found.",
            "duration": 0
        }

    try:
        # Use a new connection for each process
        with sqlite3.connect(db_path, timeout=10) as conn:
            cursor = conn.cursor()
            # Enable case-sensitive matching for the LIKE operator
            cursor.execute("PRAGMA case_sensitive_like = true;")

            # SQLite's GLOB is case-sensitive and uses '*'/'?' wildcards like file systems,
            # which is closer to the standard SQL LIKE behavior than SQLite's default LIKE.
            if "LIKE" in sql.upper():
                sql = sql.replace("LIKE", "GLOB").replace("%", "*")

            start_time = time.time()
            cursor.execute(sql)
            result = cursor.fetchall()
            end_time = time.time()
            
            duration = round(end_time - start_time, 4)

            return {
                "index": index,
                "db_id": db_id,
                "status": "success",
                "data": result,
                "duration": duration
            }
    except Exception as e:
        return {
            "index": index,
            "db_id": db_id,
            "status": "error",
            "data": str(e),
            "duration": 0
        }

def run_sql_parallel(input_file, db_dir, output_file, num_workers):
    """
    Reads SQL queries from a file and executes them in parallel.
    Saves the results and performance statistics to output files.

    Args:
        input_file (str): Path to the file containing queries.
        db_dir (str): Path to the directory containing database files.
        output_file (str): Path to save the query results.
        num_workers (int): Number of parallel processes to use.
    """
    # 1. Read all queries from the input file
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = [line.strip().split('\t') for line in f if line.strip()]
    except FileNotFoundError:
        print(f"[✗] Error: Input file not found at {input_file}")
        return

    # Prepare arguments for each worker process
    tasks = []
    for line in lines:
        if len(line) < 3:
            print(f"Warning: Skipping invalid line format -> {'\t'.join(line)}")
            continue
        # Format: (index, sql, db_id, db_dir)
        tasks.append((int(line[0]), '\t'.join(line[1:-1]), line[-1], db_dir))
    
    # 2. Execute queries in parallel using a process pool
    print(f"--> [Parallel Execution]: Executing {len(tasks)} queries with {num_workers} workers...")
    all_results = []
    with mp.Pool(processes=num_workers) as pool:
        # tqdm shows a progress bar
        all_results = list(tqdm(pool.imap_unordered(execute_single_sql, tasks), total=len(tasks)))

    # 3. Process and save the results
    # Sort results by the original index to maintain order
    all_results.sort(key=lambda r: r['index'])
    
    output_lines = []
    query_stats = {}  # db_id -> list of query execution times

    for res in all_results:
        if res['status'] == 'success':
            output_lines.append(f"{res['index']}\t{res['data']}\n")
            # Collect stats only for successful queries
            query_stats.setdefault(res['db_id'], []).append(res['duration'])
        else:
            output_lines.append(f"{res['index']}\tError: {res['data']}\n")

    with open(output_file, 'w', encoding='utf-8') as f:
        f.writelines(output_lines)
    print(f"\n[✓] All query results saved to {output_file}")

    # 4. Calculate and save execution statistics
    stats_file = output_file.replace('.txt', '_stats.csv')
    headers = ["db_id", "avg_query_time(s)", "query_count"]
    rows = []

    for db_id, times in query_stats.items():
        if times:
            avg_time = round(sum(times) / len(times), 4)
            count = len(times)
            rows.append([db_id, avg_time, count])
    
    # Sort stats by database ID for consistent output
    rows.sort(key=lambda r: r[0])

    with open(stats_file, 'w', encoding='utf-8', newline='') as f:
        f.write(','.join(headers) + '\n')
        for row in rows:
            f.write(','.join(map(str, row)) + '\n')

    print(f"[✓] Execution statistics saved to {stats_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SQL queries from a file in parallel and save the results.")
    parser.add_argument('--input_file', type=str, required=True, help="Path to the input file containing index, SQL, and db_id.")
    parser.add_argument('--db_dir', type=str, required=True, help="Path to the root database directory.")
    parser.add_argument('--output_file', type=str, default='gold_result.txt', help="Path to the output file for saving results.")
    parser.add_argument('--num_workers', type=int, default=32, help="Number of parallel processes to use.")
    
    args = parser.parse_args()

    run_sql_parallel(args.input_file, args.db_dir, args.output_file, args.num_workers)
