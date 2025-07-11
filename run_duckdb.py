import os
import duckdb
import re

def prefix_table_names(sql, db_id):
    """
    Add db_id prefix to table names in the SQL query.

    Supported keywords include FROM, JOIN, UPDATE, INTO, etc.
    """
    keywords = ['FROM', 'JOIN', 'UPDATE', 'INTO', 'DELETE FROM', 'TABLE']
    pattern = re.compile(
        r'\b(' + '|'.join(keywords) + r')\s+([`"]?)(\w+)\2',
        re.IGNORECASE
    )
    
    def replacer(match):
        keyword = match.group(1)
        quote = match.group(2)
        table = match.group(3)
        # Skip if the table name already has a prefix
        if '.' in table:
            return match.group(0)
        return f"{keyword} {db_id}.{quote}{table}{quote}"
    
    modified_sql = pattern.sub(replacer, sql)
    return modified_sql

def run_duckdb_and_save_results(gold_file, db_dir, output_file):
    results = []

    # Read the gold.txt file
    with open(gold_file, 'r') as f:
        queries = [line.strip().split('\t') for line in f.readlines() if len(line.strip()) > 0]

    # Create an in-memory DuckDB connection
    conn = duckdb.connect(database=':memory:')

    for idx, (index, sql, db_id) in enumerate(queries):
        db_path = os.path.join(db_dir, db_id, f"{db_id}.sqlite")
        
        try:
            # Attach the SQLite database to DuckDB
            attach_sql = f"ATTACH '{db_path}' AS {db_id};"
            conn.execute(attach_sql)

            # Modify the SQL by adding db_id prefix to table names
            modified_sql = prefix_table_names(sql, db_id)

            # Execute the modified SQL query
            result = conn.execute(modified_sql).fetchall()
            results.append(f"{index}\t{result}\n")
        except Exception as e:
            # Capture and record SQL execution errors
            results.append(f"{index}\tError: {str(e)}\n")
        finally:
            # Detach the SQLite database after use
            conn.execute(f"DETACH {db_id};")

    # Write results to the output file
    with open(output_file, 'w') as output:
        output.writelines(results)

    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run DuckDB SQL from a gold file and save results.")
    parser.add_argument('--gold', type=str, required=True, help="Path to the gold.txt file containing SQL and db_id.")
    parser.add_argument('--db_dir', type=str, required=True, help="Path to the database directory containing SQLite files.")
    parser.add_argument('--output', type=str, default='duckdb_result.txt', help="Path to the output file for saving results.")
    
    args = parser.parse_args()

    run_duckdb_and_save_results(args.gold, args.db_dir, args.output)
