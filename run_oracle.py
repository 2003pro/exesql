import os
import sqlite3
import oracledb
from tqdm import tqdm
import re
import argparse
import multiprocessing as mp

# --- PHASE 1: DATABASE MIGRATION WORKER ---

def sqlite_to_oracle(sqlite_file, oracle_config, db_id):
    """
    Imports a single SQLite database into Oracle.
    To avoid name collisions, target table names are prefixed as "{db_id}_{original_table_name}".
    This function is designed to be executed by a worker process.
    """
    sqlite_conn = None
    oracle_conn = None
    try:
        # Each worker process must create its own connections
        sqlite_conn = sqlite3.connect(sqlite_file)
        sqlite_cursor = sqlite_conn.cursor()

        oracle_conn = oracledb.connect(**oracle_config)
        oracle_cursor = oracle_conn.cursor()

        # Fetch all user tables from SQLite (excluding internal system tables)
        sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = [table[0] for table in sqlite_cursor.fetchall()]

        # Define a mapping from SQLite to Oracle data types
        type_mapping = {
            "INTEGER": "NUMBER(10)", "TEXT": "VARCHAR2(4000)", "REAL": "BINARY_DOUBLE",
            "BLOB": "BLOB", "NUMERIC": "NUMBER(10,2)", "BOOLEAN": "NUMBER(1)"
        }

        for table_name in tables:
            # Get table schema information
            sqlite_cursor.execute(f"PRAGMA table_info(`{table_name}`);")
            columns = sqlite_cursor.fetchall()
            if not columns: continue  # Skip empty tables

            # Construct the target Oracle table name with a db_id prefix
            # Oracle identifiers are typically uppercase and have a max length of 128 bytes.
            oracle_table = f"{db_id.upper()}_{table_name.upper()}"

            # Drop the table if it already exists (ignore errors if it doesn't)
            try:
                oracle_cursor.execute(f"DROP TABLE {oracle_table}")
            except oracledb.DatabaseError:
                pass # Table did not exist, which is fine

            # Build and execute the CREATE TABLE statement
            column_defs = []
            for col in columns:
                col_name = col[1]
                # Default to a large VARCHAR if type is unknown
                col_type = type_mapping.get(col[2].upper(), "VARCHAR2(4000)")
                column_defs.append(f'"{col_name.upper()}" {col_type}')

            create_table_sql = f"CREATE TABLE {oracle_table} ({', '.join(column_defs)})"
            oracle_cursor.execute(create_table_sql)

            # Fetch all rows from the SQLite table
            sqlite_cursor.execute(f"SELECT * FROM `{table_name}`")
            rows = sqlite_cursor.fetchall()
            if not rows: continue

            # Insert rows into the new Oracle table
            column_names = [f'"{col[1].upper()}"' for col in columns]
            # Oracle uses numbered placeholders like :1, :2, ...
            placeholders = ", ".join([f":{i+1}" for i in range(len(column_names))])
            insert_sql = f"INSERT INTO {oracle_table} ({', '.join(column_names)}) VALUES ({placeholders})"
            
            # executemany is significantly faster for bulk inserts
            oracle_cursor.executemany(insert_sql, rows)

        oracle_conn.commit()
        return db_id, "success", None
    except Exception as e:
        if oracle_conn: oracle_conn.rollback()
        return db_id, "error", str(e)
    finally:
        if sqlite_conn: sqlite_conn.close()
        if oracle_conn: oracle_conn.close()


def migrate_worker(args):
    """Wrapper to find the sqlite file and call the main migration function."""
    db_id, databases_dir, oracle_config = args
    sqlite_file = os.path.join(databases_dir, db_id, f"{db_id}.sqlite")
    if not os.path.isfile(sqlite_file):
        return db_id, "error", f"SQLite file not found at {sqlite_file}"
    return sqlite_to_oracle(sqlite_file, oracle_config, db_id)


# --- PHASE 2: QUERY EXECUTION WORKER ---

def rewrite_sql_for_oracle(sql, db_id):
    """
    Rewrites a SQL query to prefix table names with the database ID.
    This version uses regex and does not require a database connection.
    Example: FROM Customers -> FROM {DB_ID}_CUSTOMERS
    """
    # Keywords that are typically followed by a table name
    keywords = r'(FROM|JOIN|UPDATE|INTO|TABLE)\s+'
    # Pattern to find a keyword followed by a potential table name (word)
    # It captures the keyword and the table name separately.
    pattern = re.compile(keywords + r'("?)([a-zA-Z0-9_]+)("?)', re.IGNORECASE)

    def replacer(match):
        keyword = match.group(1)
        quote1 = match.group(2)
        table_name = match.group(3)
        quote2 = match.group(4)
        # Reconstruct the prefixed table name, preserving original quotes
        prefixed_table = f'"{db_id.upper()}_{table_name.upper()}"'
        return f"{keyword}{prefixed_table}"

    rewritten_sql = pattern.sub(replacer, sql)
    return rewritten_sql

def execute_sql_worker(args):
    """
    Executes a single SQL query against the Oracle database.
    Designed to be run by a worker process.
    """
    query_index, sql_query, db_id, oracle_config = args
    oracle_conn = None
    try:
        # Each worker process creates its own connection
        oracle_conn = oracledb.connect(**oracle_config)
        cursor = oracle_conn.cursor()

        # Rewrite SQL to use the prefixed table names
        rewritten_query = rewrite_sql_for_oracle(sql_query, db_id)
        
        cursor.execute(rewritten_query)
        
        # If the query was a SELECT statement, fetch results
        if cursor.description:
            rows = cursor.fetchall()
            result = rows
        else:
            # For non-SELECT queries (INSERT, UPDATE, etc.)
            oracle_conn.commit()
            result = "Query executed successfully (no rows returned)."
        
        return query_index, "success", result

    except Exception as e:
        return query_index, "error", str(e).strip()
    finally:
        if oracle_conn: oracle_conn.close()


# --- MAIN ORCHESTRATOR ---

def main():
    parser = argparse.ArgumentParser(description="Migrate SQLite DBs to Oracle and run queries in parallel.")
    parser.add_argument('--input_file', type=str, required=True, help="Path to the input file (format: index\\tSQL\\tdb_id).")
    parser.add_argument('--db_dir', type=str, required=True, help="Directory containing SQLite database subdirectories.")
    parser.add_argument('--user', type=str, default='system', help="Oracle username.")
    parser.add_argument('--password', type=str, required=True, help="Oracle password.")
    parser.add_argument('--dsn', type=str, default='localhost/FREE', help="Oracle DSN (e.g., 'localhost/FREE' or an EZCONNECT string).")
    parser.add_argument('--num_workers', type=int, default=mp.cpu_count(), help="Number of parallel processes to use.")
    args = parser.parse_args()

    oracle_config = {"user": args.user, "password": args.password, "dsn": args.dsn}
    
    # Read all queries and identify unique databases required for migration
    try:
        with open(args.input_file, 'r', encoding='utf-8') as f:
            lines = [line.strip().split('\t') for line in f if line.strip()]
            queries = [(int(parts[0]), parts[1], parts[2]) for parts in lines if len(parts) == 3]
    except (FileNotFoundError, IndexError, ValueError) as e:
        print(f"❌ Error reading or parsing input file: {e}")
        return
        
    unique_db_ids = sorted(list({q[2] for q in queries}))
    print(f"Found {len(queries)} queries across {len(unique_db_ids)} unique databases.")

    # --- PHASE 1: PARALLEL DATABASE MIGRATION ---
    print(f"\n--- Phase 1: Migrating {len(unique_db_ids)} databases using {args.num_workers} workers ---")
    migration_tasks = [(db_id, args.db_dir, oracle_config) for db_id in unique_db_ids]
    failed_migrations = set()
    
    with mp.Pool(processes=args.num_workers) as pool:
        results = list(tqdm(pool.imap_unordered(migrate_worker, migration_tasks), total=len(migration_tasks), desc="Migrating DBs"))
        for db_id, status, error_msg in results:
            if status == 'error':
                failed_migrations.add(db_id)
                print(f"\n[Warning] Migration failed for '{db_id}': {error_msg}")

    # --- PHASE 2: PARALLEL QUERY EXECUTION ---
    print(f"\n--- Phase 2: Executing {len(queries)} queries using {args.num_workers} workers ---")
    execution_tasks = [
        (idx, sql, db_id, oracle_config) for idx, sql, db_id in queries if db_id not in failed_migrations
    ]
    
    all_results = []
    with mp.Pool(processes=args.num_workers) as pool:
        all_results = list(tqdm(pool.imap_unordered(execute_sql_worker, execution_tasks), total=len(execution_tasks), desc="Executing SQL"))
        
    # Add error messages for queries whose database migration failed
    for idx, sql, db_id in queries:
        if db_id in failed_migrations:
            all_results.append((idx, "error", f"Execution skipped because database '{db_id}' failed to migrate."))
            
    # Sort results by original index to ensure correct order
    all_results.sort(key=lambda x: x[0])
    
    # --- SAVE RESULTS ---
    output_file = f"{args.input_file.rsplit('.', 1)[0]}_oracle_result.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        for index, status, data in all_results:
            if status == "success":
                f.write(f"{index}\t{data}\n")
            else:
                f.write(f"{index}\tError: {data}\n")
                
    print(f"\n✅ All tasks complete. Results saved to {output_file}")


if __name__ == "__main__":
    main()
