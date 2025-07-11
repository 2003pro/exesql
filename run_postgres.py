import os
import sqlite3
import psycopg2
from psycopg2 import sql
import re
import argparse

def prefix_table_names(sql_query, schema_name):
    """
    Prefixes table names in a SQL query with a given schema name and converts table names to lowercase.
    This function targets keywords like FROM, JOIN, UPDATE, INTO, etc.
    """
    schema_name = schema_name.lower()
    # A regex pattern to find table names following SQL keywords.
    pattern = re.compile(
        r'\b(FROM|JOIN|UPDATE|INTO|DELETE FROM|TABLE|INSERT INTO|VALUES)\s+([`"]?)(\w+)\2',
        re.IGNORECASE
    )

    def replacer(match):
        keyword = match.group(1)
        quote = match.group(2)
        table = match.group(3).lower()
        # Avoid adding a prefix if one already exists.
        if '.' in table:
            return match.group(0)
        return f"{keyword} {schema_name}.{quote}{table}{quote}"

    modified_sql = pattern.sub(replacer, sql_query)
    return modified_sql


def sqlite_to_postgres(sqlite_file, pg_conn, schema_name):
    """
    Imports data from a SQLite database into a specified schema in PostgreSQL.
    All table and column names are converted to lowercase.
    """
    schema_name = schema_name.lower()
    sqlite_conn = sqlite3.connect(sqlite_file)
    sqlite_cursor = sqlite_conn.cursor()

    # Fetch all table names from the SQLite database and convert them to lowercase.
    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [(table_name[0].lower(),) for table_name in sqlite_cursor.fetchall()]

    pg_cursor = pg_conn.cursor()
    # Create the schema in PostgreSQL, dropping it first if it exists.
    pg_cursor.execute(
        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE;").format(sql.Identifier(schema_name))
    )
    pg_cursor.execute(
        sql.SQL("CREATE SCHEMA {};").format(sql.Identifier(schema_name))
    )

    # Iterate over each table to import its structure and data into PostgreSQL.
    for (table_name,) in tables:
        sqlite_cursor.execute(f"PRAGMA table_info({table_name});")
        columns = sqlite_cursor.fetchall()
        # Define columns for the new table, converting all column names to lowercase.
        column_defs = ", ".join([f'"{col[1].lower()}" TEXT' for col in columns])

        # Create the table in the specified PostgreSQL schema.
        create_table_query = sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(column_defs),
        )
        pg_cursor.execute(create_table_query)

        # Fetch all rows from the SQLite table and insert them into the new PostgreSQL table.
        sqlite_cursor.execute(f"SELECT * FROM {table_name};")
        rows = sqlite_cursor.fetchall()

        if not rows:
            continue

        placeholders = ", ".join(["%s"] * len(rows[0]))
        insert_query = sql.SQL("INSERT INTO {}.{} VALUES ({})").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(placeholders),
        )
        # Use executemany for efficient bulk insertion.
        pg_cursor.executemany(insert_query, rows)


    pg_conn.commit()
    sqlite_conn.close()


def run_postgres_and_save_results(gold_file, db_dir, output_file, pg_conn):
    """
    Reads queries from a file, executes them in PostgreSQL after importing the relevant
    SQLite DB, and saves the results to an output file.
    The input file format is expected to be: "index \t SQL_query \t db_id"
    """
    results = []
    queries = []
    with open(gold_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            stripped = line.strip()
            if not stripped:
                continue

            parts = stripped.split('\t')
            if len(parts) != 3:
                print(f"Warning: Line {line_num} in gold file does not have three parts. Skipping: {line}")
                continue
            query_index, sql_query, db_id = parts
            queries.append((query_index, sql_query, db_id))

    if not queries:
        print("No valid queries found in the gold file.")
        return

    for query_index, sql_query, db_id in queries:
        sqlite_file = os.path.join(db_dir, db_id, f"{db_id}.sqlite")
        modified_sql = "" # Initialize to prevent reference before assignment in except block

        if not os.path.isfile(sqlite_file):
            err_msg = f"Index {query_index}:\nError: SQLite database file not found: {sqlite_file}\n"
            print(err_msg)
            results.append(err_msg)
            continue

        try:
            # Import the SQLite database into PostgreSQL for the current query.
            sqlite_to_postgres(sqlite_file, pg_conn, db_id)

            # Add the schema prefix to table names in the SQL query.
            modified_sql = prefix_table_names(sql_query, db_id)

            # Execute the modified query in PostgreSQL.
            pg_cursor = pg_conn.cursor()
            pg_cursor.execute(modified_sql)
            query_result = pg_cursor.fetchall()

            results.append(f"{query_index}\t{query_result}\n")
            pg_conn.commit()

        except Exception as e:
            # Log any errors that occur during the process.
            err_msg = (
                f"Index {query_index}:\nError: {str(e)}\n"
                f"Original SQL: {sql_query}\n"
                f"Modified SQL: {modified_sql}\n"
            )
            print(err_msg)
            results.append(err_msg)
            pg_conn.rollback()

    # Write all results to the specified output file.
    with open(output_file, 'w') as output:
        output.writelines(results)

    print(f"\nResults saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SQL queries from a file against a PostgreSQL database and save the results.")
    parser.add_argument('--gold', type=str, required=True, help="Path to the input file containing: index, SQL, db_id.")
    parser.add_argument('--db_dir', type=str, required=True, help="Path to the directory containing SQLite database folders.")
    parser.add_argument('--output', type=str, default='postgres_result.txt', help="Path to the output file for saving results.")
    parser.add_argument('--pg_host', type=str, required=True, help="PostgreSQL host.")
    parser.add_argument('--pg_port', type=str, default='5432', help="PostgreSQL port.")
    parser.add_argument('--pg_user', type=str, required=True, help="PostgreSQL username.")
    parser.add_argument('--pg_password', type=str, required=True, help="PostgreSQL password.")
    parser.add_argument('--pg_dbname', type=str, required=True, help="PostgreSQL database name.")

    args = parser.parse_args()

    # Establish the connection to PostgreSQL.
    try:
        pg_conn = psycopg2.connect(
            host=args.pg_host,
            port=args.pg_port,
            user=args.pg_user,
            password=args.pg_password,
            dbname=args.pg_dbname,
        )
    except psycopg2.OperationalError as e:
        print(f"Error: Could not connect to PostgreSQL. Please check your connection details. \n{e}")
        exit(1)


    try:
        run_postgres_and_save_results(
            gold_file=args.gold,
            db_dir=args.db_dir,
            output_file=args.output,
            pg_conn=pg_conn
        )
    finally:
        # Ensure the database connection is closed.
        if 'pg_conn' in locals() and pg_conn:
            pg_conn.close()
