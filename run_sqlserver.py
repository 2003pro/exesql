import os
import pymssql
import multiprocessing as mp
import argparse
import signal
import sys
from time import perf_counter
from tqdm import tqdm

# Recommended: install func-timeout for robust timeout handling
try:
    from func_timeout import func_timeout, FunctionTimedOut
except ImportError:
    print("Please run 'pip install func-timeout' to use the timeout feature.")
    sys.exit(1)

# --- Configuration ---
# Your Python script and SQL Server container are running on the same server,
# so the host address is 'localhost'.
# Please replace the password with the one you set when creating the Docker container.
# For security, it is highly recommended to use environment variables to store the password.
SERVER_HOST = os.environ.get('SQL_SERVER_HOST', 'localhost')
SERVER_USER = os.environ.get('SQL_SERVER_USER', 'sa')
SERVER_PASSWORD = os.environ.get('SQL_SERVER_PASSWORD', 'MyTest-DB-2024!') # <-- Replace with your actual password here

def execute_sql(index, sql, db_id):
    """
    Connects to a specific SQL Server database and executes a single SQL query.

    Args:
        index (int): The original index of the query.
        sql (str): The SQL query string to execute.
        db_id (str): The name of the database to connect to.

    Returns:
        tuple: A tuple containing (index, result, status_code).
               - result is the fetched data or an error message.
               - status_code is 1 for success, 0 for failure.
    """
    if not sql:
        return index, "SQL query is empty", 0

    conn = None  # Initialize connection variable
    try:
        # Connect to SQL Server using pymssql.
        # db_id is used directly as the database name to connect to.
        conn = pymssql.connect(
            server=SERVER_HOST,
            user=SERVER_USER,
            password=SERVER_PASSWORD,
            database=db_id,
            timeout=5,      # Connection timeout
            login_timeout=5 # Login timeout
        )
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        conn.close()
        return index, result, 1

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        if conn:
            conn.close()
        return index, error_msg, 0

def execute_sql_wrapper(index, sql, db_id, timeout):
    """
    A wrapper function to apply a timeout to the execute_sql function.

    Args:
        index (int): The original index of the query.
        sql (str): The SQL query string.
        db_id (str): The database name.
        timeout (int): Timeout in seconds.

    Returns:
        tuple: The result from execute_sql or a timeout error tuple.
    """
    try:
        res = func_timeout(timeout, execute_sql, args=(index, sql, db_id))
    except FunctionTimedOut:
        res = (index, f"Query timed out after {timeout} seconds.", 0)
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        res = (index, error_msg, 0)
    return res

def run_queries_parallel(queries, num_workers, timeout):
    """
    Executes a list of SQL queries in parallel using a multiprocessing pool.

    Args:
        queries (list): A list of tuples, where each tuple is (index, sql, db_id).
        num_workers (int): The number of parallel processes to use.
        timeout (int): The timeout for each individual query.

    Returns:
        list: A list of results, sorted by the original query index.
    """
    args = [(index, sql, db_id, timeout) for index, sql, db_id in queries]
    
    print(f"--> [Parallel Execution]: Preparing to execute {len(args)} SQL statements with {num_workers} workers...")
    start = perf_counter()
    
    # Use a multiprocessing pool to execute the wrapper function in parallel
    with mp.Pool(processes=num_workers) as pool:
        results_list = list(tqdm(pool.starmap(execute_sql_wrapper, args), total=len(args), desc="Executing SQL in parallel"))

    # Re-sort results by the original index to ensure correct order
    results = sorted(results_list, key=lambda x: x[0])
    
    end = perf_counter()
    print(f"--> [Parallel Execution]: Completed in {end - start:.2f} seconds.")
    return results

def main():
    """
    Main function to read queries, execute them, and save the results.
    """
    parser = argparse.ArgumentParser(description="Read SQL queries from a file, run them on SQL Server in parallel, and save the results.")
    parser.add_argument('--gold', type=str, required=True, help="Path to the gold.txt file (format: index\\tquery\\tdb_id).")
    parser.add_argument('--output', type=str, default='gold_result_sqlserver.txt', help="Output file name to save results.")
    parser.add_argument('--host', type=str, default=SERVER_HOST, help=f"SQL Server host address. Default: '{SERVER_HOST}'.")
    parser.add_argument('--user', type=str, default=SERVER_USER, help=f"SQL Server username. Default: '{SERVER_USER}'.")
    parser.add_argument('--password', type=str, help="SQL Server password. Recommended to use environment variables.")
    parser.add_argument('--num_workers', type=int, default=8, help="Number of parallel processes to use. Default: 8.")
    parser.add_argument('--timeout', type=int, default=30, help="Timeout in seconds for each query. Default: 30.")
    
    args = parser.parse_args()

    # Update global variables from command-line arguments
    global SERVER_HOST, SERVER_USER, SERVER_PASSWORD
    SERVER_HOST = args.host
    SERVER_USER = args.user
    if args.password:
        SERVER_PASSWORD = args.password

    print("--- SQL Server Connection Details ---")
    print(f"Host: {SERVER_HOST}")
    print(f"User: {SERVER_USER}")
    print("-------------------------------------")

    try:
        with open(args.gold, 'r', encoding='utf-8') as f:
            lines = [line.strip().split('\t') for line in f if line.strip()]
            queries = [(int(line[0]), line[1], line[2]) for line in lines if len(line) == 3]
    except FileNotFoundError:
        print(f"[✗] Error: Input file {args.gold} not found.")
        return
    except (ValueError, IndexError):
        print(f"[✗] Error: Invalid file format in {args.gold}. Expected 'index\\tquery\\tdb_id'.")
        return

    # Run queries in parallel
    results = run_queries_parallel(queries, args.num_workers, args.timeout)

    # Format and save the results to the output file
    output_lines = []
    for index, result, status in results:
        if status == 1: # Success
            output_lines.append(f"{index}\t{result}\n")
        else: # Error or Timeout
            output_lines.append(f"{index}\tError: {result}\n")
            
    with open(args.output, 'w', encoding='utf-8') as output_file:
        output_file.writelines(output_lines)

    print(f"\n[✓] All queries processed. Results have been saved to {args.output}")

if __name__ == "__main__":
    # Set start method to 'spawn' for compatibility across platforms
    mp.set_start_method("spawn", force=True)
    main()
