import os
import sqlite3
import mysql.connector
from tqdm import tqdm
import re

def sqlite_to_mysql(sqlite_file, mysql_config, db_id):
    """
    Import a SQLite database into MySQL using db_id as the target database name.
    Rows that fail to insert are skipped. Data types are mapped with lenient defaults.
    Returns the total number of inserted rows.
    """
    total_rows_inserted = 0

    sqlite_conn = sqlite3.connect(sqlite_file)
    sqlite_cursor = sqlite_conn.cursor()

    mysql_conn = mysql.connector.connect(**mysql_config)
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_id}`")
    mysql_cursor.execute(f"USE `{db_id}`")

    # Drop existing tables
    mysql_cursor.execute("SHOW TABLES;")
    for (table_name,) in mysql_cursor.fetchall():
        mysql_cursor.execute(f"DROP TABLE `{table_name}`")
    print(f"[INFO] Cleared all tables in MySQL database `{db_id}`.")

    # Get table names from SQLite (exclude system tables)
    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables = [row[0] for row in sqlite_cursor.fetchall()]

    # SQLite to MySQL type mapping
    type_mapping = {
        "INTEGER": "BIGINT",
        "TEXT": "TEXT",
        "REAL": "FLOAT",
        "BLOB": "BLOB",
        "NUMERIC": "DECIMAL(20,5)",
        "BOOLEAN": "TINYINT(1)"
    }

    for table_name in tables:
        sqlite_cursor.execute(f"PRAGMA table_info(`{table_name}`);")
        columns = sqlite_cursor.fetchall()
        if not columns:
            continue

        column_names = [f"`{col[1]}`" for col in columns]
        column_types = [col[2] for col in columns]
        target_types = [type_mapping.get(ct.upper(), 'TEXT') for ct in column_types]

        mysql_columns = [f"{name} {typ}" for name, typ in zip(column_names, target_types)]
        mysql_cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        create_sql = f"CREATE TABLE `{table_name}` ({', '.join(mysql_columns)})"
        mysql_cursor.execute(create_sql)

        sqlite_cursor.execute(f"SELECT * FROM `{table_name}`")
        rows = sqlite_cursor.fetchall()

        cleaned_rows = []
        for row in rows:
            new_row = []
            skip = False
            for value, target in zip(row, target_types):
                try:
                    if target.startswith(("BIGINT", "DECIMAL", "FLOAT", "TINYINT")):
                        if isinstance(value, str) and value.strip().upper() == "NULL":
                            new_row.append(None)
                        else:
                            new_row.append(int(value) if target.startswith(("BIGINT", "TINYINT")) else float(value))
                    else:
                        new_row.append(value)
                except Exception:
                    skip = True
                    break
            if not skip:
                cleaned_rows.append(tuple(new_row))

        if cleaned_rows:
            placeholders = ', '.join(['%s'] * len(column_names))
            insert_sql = f"INSERT INTO `{table_name}` ({', '.join(column_names)}) VALUES ({placeholders})"
            try:
                mysql_cursor.executemany(insert_sql, cleaned_rows)
            except mysql.connector.Error as e:
                print(f"[ERROR] Batch insert failed on table `{table_name}`: {e}")
                for idx, row in enumerate(cleaned_rows):
                    try:
                        mysql_cursor.execute(insert_sql, row)
                    except mysql.connector.Error as e2:
                        print(f"  [Row {idx}] Skipped: {e2}")
            total_rows_inserted += len(cleaned_rows)

    mysql_conn.commit()
    sqlite_conn.close()
    mysql_conn.close()

    return total_rows_inserted

def run_mysql_and_save_results(gold_file, db_dir, mysql_config, output_file):
    """
    Reads SQLs from a gold file in the format "index<TAB>SQL<TAB>db_id",
    imports corresponding SQLite databases into MySQL,
    runs the SQL query, and writes the result to an output file.
    """
    results = []
    imported_db_ids = set()

    questions = []
    with open(gold_file, 'r') as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) == 3:
                questions.append(parts)
            else:
                print(f"[WARNING] Skipped malformed line: {line.strip()}")

    for i in tqdm(range(len(questions)), desc="Executing SQL queries"):
        query_index, sql_query, db_id = questions[i]
        sqlite_file = os.path.join(db_dir, db_id, f"{db_id}.sqlite")

        if db_id not in imported_db_ids:
            if not os.path.isfile(sqlite_file):
                results.append(f"{query_index}\tError: SQLite file not found: {sqlite_file}\n")
                continue
            try:
                sqlite_to_mysql(sqlite_file, mysql_config, db_id)
                imported_db_ids.add(db_id)
            except Exception as e:
                results.append(f"{query_index}\tError importing {db_id}: {str(e)}\n")
                continue

        try:
            mysql_conn = mysql.connector.connect(**mysql_config)
            mysql_cursor = mysql_conn.cursor()
            mysql_cursor.execute(f"USE `{db_id}`")
            mysql_cursor.execute("SET SESSION max_execution_time = 60000;")
            mysql_cursor.execute(sql_query)
            result = mysql_cursor.fetchall()
            results.append(f"{query_index}\t{result}\n")
            mysql_conn.commit()
            mysql_conn.close()
        except mysql.connector.Error as err:
            error_msg = "Error: Query timed out after 1 minute." if "maximum execution time" in str(err) else f"Error executing query: {str(err)}"
            results.append(f"{query_index}\t{error_msg}\tSQL: {sql_query}\n")
        except Exception as e:
            results.append(f"{query_index}\tError executing query: {str(e)}\tSQL: {sql_query}\n")

    with open(output_file, 'w') as f:
        f.writelines(results)
    print(f"\n[✓] Results saved to {output_file}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--gold', type=str, required=True, help="Path to gold file (index\\tSQL\\tdb_id).")
    parser.add_argument('--db_dir', type=str, required=True, help="Path to SQLite databases.")
    parser.add_argument('--mysql_user', type=str, required=True)
    parser.add_argument('--mysql_password', type=str, required=True)
    parser.add_argument('--mysql_socket', type=str, required=True)
    parser.add_argument('--output', type=str, default='gold_result.txt', help="Output file path.")

    args = parser.parse_args()

    mysql_config = {
        "unix_socket": args.mysql_socket,
        "user": args.mysql_user,
        "password": args.mysql_password,
        "port": 3307
    }

    run_mysql_and_save_results(
        gold_file=args.gold,
        db_dir=args.db_dir,
        mysql_config=mysql_config,
        output_file=args.output
    )
