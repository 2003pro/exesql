import os
import sqlite3
import pymssql
import argparse
from tqdm import tqdm

# --- Configuration ---
# Please ensure the connection details here match your SQL Server Docker container
SERVER_HOST = 'localhost'
SERVER_USER = 'sa'
# !!! WARNING: Be sure to replace the password below with your own actual password !!!
SERVER_PASSWORD = 'MyTest-DB-2024!'

def translate_type(sqlite_type):
    """Translates SQLite data types to SQL Server compatible data types."""
    # Default to NVARCHAR(MAX) if the type is undefined
    if sqlite_type is None:
        return 'NVARCHAR(MAX)'
    
    sqlite_type = sqlite_type.upper()
    if 'INT' in sqlite_type:
        return 'BIGINT' # Use BIGINT to be compatible with all integer sizes
    if 'CHAR' in sqlite_type or 'TEXT' in sqlite_type or 'CLOB' in sqlite_type:
        return 'NVARCHAR(MAX)'
    if 'REAL' in sqlite_type or 'FLOAT' in sqlite_type or 'DOUBLE' in sqlite_type:
        return 'FLOAT'
    if 'BLOB' in sqlite_type:
        return 'VARBINARY(MAX)'
    if 'NUMERIC' in sqlite_type:
        return 'DECIMAL(38, 9)' # Increase precision to accommodate more types
    if 'DATE' in sqlite_type:
        return 'DATETIME2'
    
    return 'NVARCHAR(MAX)' # Default type

def migrate_database(db_id, db_path):
    """Migrates a single SQLite database to SQL Server."""
    print(f"\n--- Starting migration for database: {db_id} ---")

    # 1. Connect to the master SQL Server instance to create the new database
    master_conn = None
    try:
        master_conn = pymssql.connect(server=SERVER_HOST, user=SERVER_USER, password=SERVER_PASSWORD, database='master', login_timeout=10)
        master_conn.autocommit(True) # Set to autocommit mode
        cursor = master_conn.cursor()

        # Check if the database already exists, and create it if it doesn't
        cursor.execute(f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'{db_id}') CREATE DATABASE [{db_id}];")
        print(f"[✓] Ensured database '{db_id}' exists in SQL Server.")
        cursor.close()
    except Exception as e:
        print(f"[✗] Error: Failed to create database '{db_id}': {e}")
        return
    finally:
        if master_conn:
            master_conn.close()

    # 2. Connect to the SQLite database
    try:
        sqlite_conn = sqlite3.connect(db_path)
        sqlite_cursor = sqlite_conn.cursor()
    except Exception as e:
        print(f"[✗] Error: Could not connect to SQLite file '{db_path}': {e}")
        return

    # 3. Connect to the newly created database in SQL Server
    sql_server_conn = None
    try:
        sql_server_conn = pymssql.connect(server=SERVER_HOST, user=SERVER_USER, password=SERVER_PASSWORD, database=db_id, login_timeout=10)
        sql_server_cursor = sql_server_conn.cursor()

        # Get all table names
        sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = [row[0] for row in sqlite_cursor.fetchall()]
        
        print(f"Found {len(tables)} tables to migrate: {tables}")

        for table_name in tables:
            print(f"  - Processing table: {table_name}")

            # Create the table in SQL Server
            sqlite_cursor.execute(f"PRAGMA table_info(`{table_name}`);")
            columns = sqlite_cursor.fetchall()

            create_table_sql = f"CREATE TABLE [{table_name}] ("
            column_definitions = [f"[{col[1]}] {translate_type(col[2])}" for col in columns]
            create_table_sql += ", ".join(column_definitions) + ");"

            try:
                # First, try to drop the table if it exists, to allow for re-migration
                sql_server_cursor.execute(f"DROP TABLE IF EXISTS [{table_name}];")
                sql_server_cursor.execute(create_table_sql)
            except Exception as e:
                print(f"    [✗] Error: Failed to create table '{table_name}': {e}")
                continue

            # Read data from SQLite
            sqlite_cursor.execute(f"SELECT * FROM `{table_name}`;")
            original_rows = sqlite_cursor.fetchall()

            if not original_rows:
                print(f"    [!] Table '{table_name}' is empty, skipping insertion.")
                continue

            # ==================== NEW DATA CLEANING STEP ====================
            # Replace empty strings '' in each row with None to correctly insert as SQL NULL.
            cleaned_rows = []
            for row in original_rows:
                # Use a tuple comprehension to efficiently process each row
                cleaned_row = tuple(None if item == '' else item for item in row)
                cleaned_rows.append(cleaned_row)
            # ==================================================================

            placeholders = ", ".join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO [{table_name}] VALUES ({placeholders})"

            try:
                # Use the cleaned data (cleaned_rows) for insertion
                sql_server_cursor.executemany(insert_sql, cleaned_rows)
                sql_server_conn.commit()
                print(f"    [✓] Successfully migrated {len(cleaned_rows)} rows to table '{table_name}'.")
            except Exception as e:
                print(f"    [✗] Error: Failed to insert data into table '{table_name}': {e}")
                sql_server_conn.rollback()

    except Exception as e:
        print(f"[✗] A critical error occurred during the migration of database '{db_id}': {e}")
    finally:
        if sqlite_conn:
            sqlite_conn.close()
        if sql_server_conn:
            sql_server_conn.close()

def main():
    parser = argparse.ArgumentParser(description="Migrate a directory of SQLite databases to SQL Server.")
    parser.add_argument('--db_dir', type=str, required=True, help="Root directory containing SQLite database subdirectories.")
    
    args = parser.parse_args()

    if not os.path.isdir(args.db_dir):
        print(f"[✗] Error: Directory '{args.db_dir}' does not exist.")
        return

    # Find all database directories
    db_ids = [name for name in os.listdir(args.db_dir) if os.path.isdir(os.path.join(args.db_dir, name))]
    
    print(f"Found {len(db_ids)} databases to migrate.")

    # Overall migration progress
    for db_id in tqdm(db_ids, desc="Overall Migration Progress"):
        db_path = os.path.join(args.db_dir, db_id, f"{db_id}.sqlite")
        if os.path.exists(db_path):
            migrate_database(db_id, db_path)
        else:
            print(f"[!] Warning: SQLite file not found in directory '{db_id}': {db_path}")

    print("\n[🎉] All database migrations are complete!")

if __name__ == "__main__":
    main()
