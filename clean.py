import argparse
import re

def process_file(input_file, output_file):
    """
    Reads the input file, merges multi-line SQL, cleans it, 
    and writes to the output file.
    """
    records = []
    current_line = ""

    with open(input_file, "r", encoding="utf-8") as infile:
        for line in infile:
            line = line.strip()
            if not line:
                continue

            # If the line starts with an index (digits + tab), it's a new record
            if re.match(r'^\d+\t', line):
                if current_line:
                    records.append(current_line)
                current_line = line
            else:
                # Otherwise, it's part of the previous SQL, append to the current line
                current_line += " " + line

        # Don't forget to add the last record
        if current_line:
            records.append(current_line)

    skipped_count = 0
    with open(output_file, "w", encoding="utf-8") as outfile:
        for line in records:
            parts = line.strip().split("\t")
            if len(parts) < 3:
                skipped_count += 1
                print(f"Skipping malformed line: {' '.join(parts)}")
                continue  # Skip malformed lines

            index, sql_text, db_id = parts[0], "\t".join(parts[1:-1]), parts[-1]

            # Remove newlines from the SQL string
            sql_text = sql_text.replace("\n", " ").replace("\r", " ")

            # Extract the first SQL statement starting with SELECT
            match = re.search(r'(?i)(SELECT.*?)(?=;|```|$)', sql_text, re.DOTALL)
            if match:
                cleaned_sql = match.group(1).strip()
            else:
                cleaned_sql = sql_text.strip()

            # ### MODIFICATION START ###
            # Check if the SQL is empty after extraction. If so, skip this line.
            cleaned_sql = cleaned_sql.split("###")[0].strip()

            if cleaned_sql:
                outfile.write(f"{index}\t{cleaned_sql}\t{db_id}\n")
            else:
                skipped_count += 1
                print(f"Skipping record with index {index} due to missing SQL.")
            # ### MODIFICATION END ###

    print(f"\nProcessing complete. Output file: {output_file}")
    if skipped_count > 0:
        print(f"Skipped {skipped_count} records due to malformed data or missing SQL.")


def main():
    parser = argparse.ArgumentParser(
        description="Clean SQL text: merge multi-line SQL, extract the first SELECT statement, and format as index<TAB>SQL<TAB>db_id"
    )
    parser.add_argument("--input_file", required=True, help="Path to the input .txt file")
    parser.add_argument("--output_file", required=True, help="Path to the output .txt file")
    args = parser.parse_args()
    process_file(args.input_file, args.output_file)

if __name__ == "__main__":
    main()
