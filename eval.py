import csv
import sys
import ast
import os
import argparse

# Increase the field size limit for reading large fields in CSV files.
csv.field_size_limit(sys.maxsize)

def normalize_value(value):
    """
    Normalizes a value by attempting to convert it to a float and round it.
    If conversion fails, it returns the value as a stripped string.
    """
    try:
        # Convert to float if it's not already a number, then round.
        num = float(value) if not isinstance(value, (int, float)) else float(value)
        return round(num, 8)
    except (ValueError, TypeError):
        return str(value).strip()

def read_result_file(file_path):
    """
    Reads a result file where each line is formatted as: index<TAB>list.
    It uses `ast.literal_eval` to parse the list part, normalizes each
    value within each tuple, and returns a dictionary mapping the index
    to a set of normalized tuples.
    """
    data = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        for line in reader:
            if len(line) < 2:
                continue
            index = line[0].strip()
            try:
                # Safely evaluate the string representation of the list.
                items = ast.literal_eval(line[1].strip())
                normalized_items = set()
                for tup in items:
                    # Ensure the item is a tuple for consistent processing.
                    if not isinstance(tup, tuple):
                        tup = (tup,)
                    normalized_tup = tuple(normalize_value(x) for x in tup)
                    normalized_items.add(normalized_tup)
                data[index] = normalized_items
            except (ValueError, SyntaxError):
                # If parsing fails, store the raw string value.
                data[index] = line[1].strip()
    return data

def read_sql_dict(file_path):
    """
    Reads a SQL file where each line is formatted as: index<TAB>SQL<TAB>db_id.
    Returns a dictionary mapping the index to a tuple of (SQL, db_id).
    """
    data = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            if len(row) < 3:
                continue
            index, sql, db_id = row[0].strip(), row[1].strip(), row[2].strip()
            data[index] = (sql, db_id)
    return data

def normalize_tuple_order_insensitive(tup):
    """
    Normalizes a tuple by normalizing each element and then sorting them
    to ensure order-insensitivity.
    """
    normalized = [normalize_value(x) for x in tup]
    # Sort the normalized elements to handle order differences.
    # Sorting is based on type and then value to avoid type-related comparison errors.
    return tuple(sorted(normalized, key=lambda x: (str(type(x)), str(x))))

def compute_exec_score(gold_file, pred_file):
    """
    Computes the execution score by comparing normalized results from a
    gold standard file and a prediction file.
    """
    gold_data = read_result_file(gold_file)
    pred_data = read_result_file(pred_file)
    
    total = len(gold_data)
    if total == 0:
        return 0.0, 0, 0
        
    match_count = 0
    for index, gold_val in gold_data.items():
        pred_val = pred_data.get(index)

        # Compare sets of tuples for equivalence.
        if isinstance(gold_val, set) and isinstance(pred_val, set):
            # Normalize tuples for order-insensitive comparison.
            norm_gold = set(normalize_tuple_order_insensitive(tup) for tup in gold_val)
            norm_pred = set(normalize_tuple_order_insensitive(tup) for tup in pred_val)
            if norm_gold == norm_pred:
                match_count += 1
        # Fallback to direct comparison for other types (e.g., raw strings).
        elif gold_val == pred_val:
            match_count += 1
            
    score = match_count / total if total > 0 else 0.0
    return score, match_count, total

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate execution accuracy score.")
    parser.add_argument('--gold_result', type=str, required=True, help="Path to the gold result file (format: index<TAB>list).")
    parser.add_argument('--pred_result', type=str, required=True, help="Path to the prediction result file (format: index<TAB>list).")
    
    args = parser.parse_args()
    
    # Compute the execution score and get the detailed counts.
    exec_score, correct_count, total_count = compute_exec_score(args.gold_result, args.pred_result)

    eval_file = "eval_score.txt"
    # Append the scores to the evaluation file, do not overwrite.
    with open(eval_file, "a", encoding="utf-8") as f:
        f.write(f"- {os.path.basename(args.pred_result)}\n")
        f.write(f"  exec_score: {exec_score:.4f} ({correct_count} / {total_count})\n")
    
    print(f"Scores have been appended to {eval_file}")
