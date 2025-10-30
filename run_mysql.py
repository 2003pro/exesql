import os
import sqlite3
import mysql.connector
from tqdm import tqdm
import argparse
import multiprocessing as mp

# --- PHASE 1: DATABASE MIGRATION ---

def sqlite_to_mysql(db_id, sqlite_file, mysql_config):
    """
    Imports a single SQLite database into MySQL.
    The target MySQL database will be named after the db_id.
    This function is designed to be run as a worker process.
    """
    try:
        # Each process needs its own connections
        sqlite_conn = sqlite3.connect(sqlite_file)
        sqlite_cursor = sqlite_conn.cursor()

        mysql_conn = mysql.connector.connect(**mysql_config)
        mysql_cursor = mysql_conn.cursor(buffered=True)
        
        # Create the database and drop all existing tables for a clean import
        mysql_cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_id}`")
        mysql_cursor.execute(f"USE `{db_id}`")
        mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        mysql_cursor.execute("SHOW TABLES;")
        for (table_name,) in mysql_cursor.fetchall():
            mysql_cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

        # Get table names from SQLite (exclude system tables)
        sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = [row[0] for row in sqlite_cursor.fetchall()]

        # Lenient SQLite to MySQL type mapping
        type_mapping = {
            "INTEGER": "BIGINT", "REAL": "DOUBLE", "TEXT": "TEXT",
            "BLOB": "BLOB", "NUMERIC": "DECIMAL(38,10)", "BOOLEAN": "TINYINT(1)"
        }

        for table_name in tables:
            sqlite_cursor.execute(f"PRAGMA table_info(`{table_name}`);")
            columns = sqlite_cursor.fetchall()
            if not columns: continue

            # Create table in MySQL
            column_defs = []
            for col in columns:
                col_name = col[1]
                # Fallback to TEXT for unknown types
                col_type = type_mapping.get(col[2].upper(), "TEXT") 
                column_defs.append(f"`{col_name}` {col_type}")
            
            create_sql = f"CREATE TABLE `{table_name}` ({', '.join(column_defs)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            mysql_cursor.execute(create_sql)

            # Insert data
            sqlite_cursor.execute(f"SELECT * FROM `{table_name}`")
            rows = sqlite_cursor.fetchall()
            if not rows: continue

            placeholders = ', '.join(['%s'] * len(columns))
            column_names = ', '.join([f"`{col[1]}`" for col in columns])
            insert_sql = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders})"
            
            try:
                # executemany is much faster than one by one
                mysql_cursor.executemany(insert_sql, rows)
            except mysql.connector.Error as e:
                print(f"Warning: Batch insert failed for {db_id}.{table_name}: {e}. Trying row-by-row.")
                # Fallback to individual inserts if batch fails
                for row in rows:
                    try:
                        mysql_cursor.execute(insert_sql, row)
                    except mysql.connector.Error:
                        pass # Skip rows that fail to insert

        mysql_conn.commit()
        
    except Exception as e:
        return db_id, "error", str(e)
    finally:
        # Ensure connections are always closed
        if 'sqlite_conn' in locals(): sqlite_conn.close()
        if 'mysql_conn' in locals(): mysql_conn.close()
        
    return db_id, "success", None

def migrate_worker(args):
    """Wrapper function for parallel execution of sqlite_to_mysql."""
    db_id, db_dir, mysql_config = args
    sqlite_file = os.path.join(db_dir, db_id, f"{db_id}.sqlite")
    if not os.path.isfile(sqlite_file):
        return db_id, "error", f"SQLite file not found: {sqlite_file}"
    return sqlite_to_mysql(db_id, sqlite_file, mysql_config)


# --- PHASE 2: QUERY EXECUTION ---

def execute_sql_worker(args):
    """
    Executes a single SQL query in a MySQL database.
    Designed to be run as a worker process.
    """
    query_index, sql_query, db_id, mysql_config = args
    try:
        # Each process needs its own connection
        mysql_conn = mysql.connector.connect(**mysql_config)
        mysql_cursor = mysql_conn.cursor()
        mysql_cursor.execute(f"USE `{db_id}`")
        
        # Set a timeout for the query (in milliseconds)
        mysql_cursor.execute("SET SESSION max_execution_time = 60000;") # 60 seconds
        mysql_cursor.execute(sql_query)
        result = mysql_cursor.fetchall()
        
        return query_index, "success", result

    except mysql.connector.Error as err:
        if "maximum execution time" in str(err).lower():
            return query_index, "error", "Query timed out after 60 seconds."
        return query_index, "error", f"MySQL Error: {err}"
    except Exception as e:
        return query_index, "error", str(e)
    finally:
        if 'mysql_conn' in locals():
            mysql_conn.close()


# --- MAIN ORCHESTRATOR ---

def main(args):
    """
    Main function to orchestrate the parallel migration and execution.
    """
    mysql_config = {
        "unix_socket": args.mysql_socket,
        "user": args.mysql_user,
        "password": args.mysql_password,
        "port": 3307  # As specified in original script
    }

    # Read all questions and identify unique databases needed
    try:
        with open(args.input_file, 'r', encoding='utf-8') as f:
            questions = [line.strip().split('\t') for line in f if len(line.strip().split('\t')) == 3]
    except FileNotFoundError:
        print(f"[✗] Error: Input file not found at '{args.input_file}'")
        return

    unique_db_ids = sorted(list({q[2] for q in questions}))
    print(f"Found {len(questions)} queries across {len(unique_db_ids)} unique databases.")

    # --- PHASE 1: PARALLEL DATABASE MIGRATION ---
    print("\n--- Starting Phase 1: Migrating databases in parallel ---")
    migration_tasks = [(db_id, args.db_dir, mysql_config) for db_id in unique_db_ids]
    
    with mp.Pool(processes=args.num_workers) as pool:
        migration_results = list(tqdm(pool.imap_unordered(migrate_worker, migration_tasks), total=len(migration_tasks), desc="Migrating DBs"))

    failed_migrations = {res[0] for res in migration_results if res[1] == 'error'}
    if failed_migrations:
        print(f"[!] Warning: Failed to migrate {len(failed_migrations)} databases. Queries against them will fail.")
        for res in migration_results:
            if res[1] == 'error':
                print(f"  - {res[0]}: {res[2]}")
    
    # --- PHASE 2: PARALLEL QUERY EXECUTION ---
    print("\n--- Starting Phase 2: Executing queries in parallel ---")
    # Filter out queries for databases that failed to migrate
    execution_tasks = [
        (int(q[0]), q[1], q[2], mysql_config) for q in questions if q[2] not in failed_migrations
    ]

    all_results = []
    with mp.Pool(processes=args.num_workers) as pool:
        all_results = list(tqdm(pool.imap_unordered(execute_sql_worker, execution_tasks), total=len(execution_tasks), desc="Executing SQL"))
    
    # Add errors for queries whose DB migration failed
    for q in questions:
        if q[2] in failed_migrations:
            all_results.append((int(q[0]), "error", f"Database '{q[2]}' failed to migrate."))

    # Sort results by the original index to ensure correct order
    all_results.sort(key=lambda x: x[0])

    # --- SAVE RESULTS ---
    with open(args.output_file, 'w', encoding='utf-8') as f:
        for index, status, data in all_results:
            if status == "success":
                f.write(f"{index}\t{data}\n")
            else:
                f.write(f"{index}\tError: {data}\n")

    print(f"\n[✓] All tasks complete. Results saved to {args.output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate SQLite DBs to MySQL and execute queries in parallel.")
    parser.add_argument('--input_file', type=str, required=True, help="Path to input file (index\\tSQL\\tdb_id).")
    parser.add_argument('--db_dir', type=str, required=True, help="Path to directory containing SQLite databases.")
    parser.add_argument('--mysql_user', type=str, required=True, help="MySQL username.")
    parser.add_argument('--mysql_password', type=str, required=True, help="MySQL password.")
    parser.add_argument('--mysql_socket', type=str, required=True, help="Path to MySQL unix_socket file.")
    parser.add_argument('--output_file', type=str, default='gold_result_mysql.txt', help="Output file path.")
    parser.add_argument('--num_workers', type=int, default=mp.cpu_count(), help="Number of parallel processes to use.")
    
    args = parser.parse_args()
    main(args)
