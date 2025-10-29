import os
import json
import glob
import sqlite3
import re
import argparse
from vllm import LLM, SamplingParams
from transformers import AutoTokenizer

# Functions for schema extraction

def minify_sql_schema(sql_text: str) -> str:
    """
    Compresses an SQL schema string by removing unnecessary whitespace, newlines,
    and comments to reduce token count.
    """
    lines = sql_text.splitlines()
    processed_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith('--'):
            processed_lines.append(stripped)
    single = ' '.join(processed_lines)
    return re.sub(r'\s+', ' ', single).strip()


def get_database_schema(db_id: str, base_db_path: str) -> str:
    """
    Retrieves and minifies the CREATE TABLE schema for a given db_id.
    """
    sqlite_file = os.path.join(base_db_path, db_id, f"{db_id}.sqlite")
    if not os.path.exists(sqlite_file):
        print(f"Warning: Could not find '{sqlite_file}' for db_id '{db_id}'")
        return ""
    try:
        conn = sqlite3.connect(sqlite_file)
        cursor = conn.cursor()
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
        stmts = cursor.fetchall()
        conn.close()
        full = ' '.join(s[0] for s in stmts if s and s[0])
        return minify_sql_schema(full)
    except Exception as e:
        print(f"Error generating schema for db_id '{db_id}': {e}")
        return ""

# Generation utilities

def generate_vllm_outputs(prompts, model_name_or_path, temperature, top_p, stop_tokens,
                          gpu_num, max_tokens, gpu_memory_utilization, dtype, batch_size):
    tokenizer = AutoTokenizer.from_pretrained(model_name_or_path, trust_remote_code=True)
    model_max = tokenizer.model_max_length
    max_tok = min(max_tokens, model_max)
    sampling = SamplingParams(
        temperature=temperature,
        top_p=top_p,
        max_tokens=max_tok,
        stop=[stop_tokens] if stop_tokens else None
    )
    llm = LLM(
        model=model_name_or_path,
        tensor_parallel_size=gpu_num,
        dtype=dtype,
        gpu_memory_utilization=gpu_memory_utilization,
        trust_remote_code=True
    )
    all_out = []
    for i in range(0, len(prompts), batch_size):
        batch = prompts[i:i+batch_size]
        out = llm.generate(batch, sampling, use_tqdm=True)
        all_out.extend(out)
    return all_out

# Prompt construction

def construct_prompt(question: str, evidence: str, schema: str, sql_dialect: str) -> str:
    """
    Constructs the prompt with the specified SQL dialect.
    """
    return (
        f"You need to generate {sql_dialect} SQL based on the following question and database schema. "
        f"The output should only be the {sql_dialect} SQL in one line beginning with SELECT, without explanations or comments.\n\n"
        f"### Question:\n{question}\n\n"
        f"### Evidence:\n{evidence}\n\n"
        f"### Database schema:\n{schema}\n\n"
        f"{sql_dialect} SQL: "
    )

# Main script

def main():
    parser = argparse.ArgumentParser(
        description="Generate SQL using local LLM and full database schema."
    )
    parser.add_argument('--data_file', type=str, required=True, help="Input JSON file path")
    parser.add_argument('--output_file', type=str, required=True, help="Output TXT file: index<TAB>SQL<TAB>db_id")
    parser.add_argument('--gpu', type=str, default=None, help="CUDA_VISIBLE_DEVICES setting")
    parser.add_argument('--model_name_or_path', type=str, required=True, help="Model name or path")
    
    # New argument for SQL dialect
    parser.add_argument(
        '--sql_dialect', 
        type=str, 
        default="Sqlite", 
        help="The SQL dialect to generate (e.g., Sqlite, PostgreSQL, MySQL). Default: Sqlite"
    )
    
    parser.add_argument('--temperature', type=float, default=0.1, help="Sampling temperature")
    parser.add_argument('--max_tokens', type=int, default=1024, help="Max tokens to generate")
    parser.add_argument('--top_p', type=float, default=0.95, help="Top-p sampling")
    parser.add_argument('--stop_tokens', type=str, default=";", help="Stop tokens")
    parser.add_argument('--gpu_memory_utilization', type=float, default=0.9, help="GPU memory utilization")
    parser.add_argument('--dtype', type=str, default="float16", choices=["float16","bfloat16"], help="Data type")
    parser.add_argument('--batch_size', type=int, default=16, help="Batch size for generation")
    parser.add_argument('--db_base_dir', type=str, default="spider_data/test_database", help="Database root directory")
    args = parser.parse_args()

    gpu_num = 1 # Default to 1 GPU
    if args.gpu:
        os.environ['CUDA_VISIBLE_DEVICES'] = args.gpu
        # Automatically determine the number of GPUs from the --gpu argument
        gpu_num = len(args.gpu.split(','))
        print(f"Set CUDA_VISIBLE_DEVICES to '{args.gpu}'. Number of GPUs: {gpu_num}")

    with open(args.data_file, 'r', encoding='utf-8') as f:
        items = json.load(f)

    prompts = []
    metas = []
    for item in items:
        idx = str(item.get('Index', item.get('question_id', '')))
        db_id = item.get('db_id', '')
        question = item.get('question', '')
        evidence = item.get('evidence', '')
        schema = get_database_schema(db_id, args.db_base_dir)
        if not schema:
            print(f"Skipping {db_id}: schema not found.")
            continue
        
        # Pass the sql_dialect to the prompt constructor
        prompt = construct_prompt(question, evidence, schema, args.sql_dialect)
        prompts.append(prompt)
        metas.append((idx, db_id))

    outputs = generate_vllm_outputs(
        prompts,
        model_name_or_path=args.model_name_or_path,
        temperature=args.temperature,
        top_p=args.top_p,
        stop_tokens=args.stop_tokens,
        gpu_num=gpu_num, # Pass the automatically calculated gpu_num
        max_tokens=args.max_tokens,
        gpu_memory_utilization=args.gpu_memory_utilization,
        dtype=args.dtype,
        batch_size=args.batch_size
    )

    with open(args.output_file, 'w', encoding='utf-8') as f:
        for (idx, db_id), out in zip(metas, outputs):
            text = ''
            if hasattr(out, 'outputs') and out.outputs:
                parts = [p.text.strip().replace('\n',' ') for p in out.outputs if hasattr(p, 'text')]
                text = ' '.join(parts)
            elif hasattr(out, 'text'):
                text = out.text.strip().replace('\n',' ')
            
            # Ensure the final output doesn't start with the dialect name if the model repeats it
            if text.upper().startswith(args.sql_dialect.upper() + " SQL:"):
                text = text[len(args.sql_dialect) + 5:].strip()

            f.write(f"{idx}\t{text}\t{db_id}\n")
    print(f"Outputs saved to {args.output_file}")

if __name__ == '__main__':
    main()
