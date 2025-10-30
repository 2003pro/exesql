# Exesql: Self-Taught Text-to-SQL Models with Execution-Driven Bootstrapping for SQL Dialects
This repository contains the official implementation of ExeSQL.

## 🧭 Overview
![Pipeline](fig/main.png)

**Figure:** *Pipeline for Dialect Text-to-SQL Data Generation and Model Training.*  
The framework consists of three stages:  
(1) **Translation Bootstrapping**: A bootstrap text-to-SQL model is fine-tuned using SQL translations from an existing dataset (e.g., SQLite) to other dialects (e.g., MySQL, PostgreSQL).  
(2) **Iterative Data Generation and Training**: The model generates multiple SQL candidates per question, which are validated via execution feedback. Correct queries are retained to refine the dataset, enabling iterative self-improvement.  
(3) **Preference Enhancement**: A Direct Preference Optimization (DPO) step is applied to distinguish correct and incorrect SQL queries. High-quality pairs (question, correct SQL) are used to further improve the model’s performance and preference learning, ensuring both correctness and efficiency in SQL generation.

## 📊 Main Results
### Performance Comparison of LLMs on SQL Benchmarks

**General Purposed LLM**

| Method               | Model size | PostgreSQL Spider | PostgreSQL WikiSQL | MySQL Spider | MySQL WikiSQL | MySQL Bird | Oracle Spider | Average |
|----------------------|------------|-------------------|--------------------|--------------|----------------|-------------|----------------|---------|
| GPT-4o               | -          | 54.59             | 58.97              | 62.09        | 57.24          | 36.38       | 64.86          | 55.69   |
| Gemini-1.5-pro       | -          | 51.03             | 54.10              | 64.90        | 51.95          | 36.11       | 65.21          | 53.88   |
| Llama3.1-Instruct    | 8B         | 33.63             | 31.60              | 48.86        | 25.41          | 24.58       | 30.00          | 32.35   |

**Code Expert LLM**

| Method               | Model size | PostgreSQL Spider | PostgreSQL WikiSQL | MySQL Spider | MySQL WikiSQL | MySQL Bird | Oracle Spider | Average |
|----------------------|------------|-------------------|--------------------|--------------|----------------|-------------|----------------|---------|
| Deepseek-Coder       | 7B         | 37.31             | 18.12              | 49.60        | 24.67          | 16.00       | 50.77          | 32.75   |
| Qwen-Coder           | 7B         | 36.80             | 15.48              | 39.04        | 22.84          | 15.36       | 58.31          | 31.31   |
| Magicoder            | 7B         | 21.90             | 17.45              | 47.28        | 23.32          | 13.23       | 26.60          | 24.96   |
| WizardCoder          | 15B        | 23.78             | 16.91              | 32.36        | 20.56          | 18.38       | 36.33          | 24.72   |

**SQL Expert LLM**

| Method               | Model size | PostgreSQL Spider | PostgreSQL WikiSQL | MySQL Spider | MySQL WikiSQL | MySQL Bird | Oracle Spider | Average |
|----------------------|------------|-------------------|--------------------|--------------|----------------|-------------|----------------|---------|
| CodeS                | 7B         | 24.76             | 20.00              | 35.60        | 23.00          | 14.41       | 37.40          | 25.86   |
| StructLLM            | 7B         | 38.71             | 30.97              | 44.20        | 7.14           | 22.69       | 33.16          | 29.48   |

**Our Method**

| Method               | Model size | PostgreSQL Spider | PostgreSQL WikiSQL | MySQL Spider | MySQL WikiSQL | MySQL Bird | Oracle Spider | Average |
|----------------------|------------|-------------------|--------------------|--------------|----------------|-------------|----------------|---------|
| **ExeSQL**           | 7B         | **69.86**         | **74.10**          | **72.09**    | **73.64**      | **41.13**   | **69.35**      | **66.70** |

## 📚 Citation
```bibtex
@article{zhang2025exesql,
  title={ExeSQL: Self-Taught Text-to-SQL Models with Execution-Driven Bootstrapping for SQL Dialects},
  author={Zhang, Jipeng and Yang, Haolin and Miao, Kehao and Zhang, Ruiyuan and Pi, Renjie and Gao, Jiahui and Zhou, Xiaofang},
  journal={arXiv preprint arXiv:2505.17231},
  year={2025}
}
```

## 🧩 Implemention

### 1. Environment Setup

First, ensure you have all the necessary dependencies installed. We highly recommend doing this within a virtual environment (e.g., `venv` or `conda`).

```bash
# (Optional, but recommended) Create and activate a new virtual environment
conda create -n exesql python=3.9 -y
conda activate exesql

# Install all required packages
cd exesql
pip install -r requirements.txt
```

### 2. Run Inference
You can use the local_generate_sql.py script to generate SQL queries. This script utilizes vLLM for high-throughput inference. You need to download the Spider dataset from [the page](https://yale-lily.github.io/spider) and Bird dataset from [the page](https://bird-bench.github.io/).

Run the code:
```bash
python local_generate_sql.py \
    --data_file spider_data/test_index.json \
    --output_file /path/to/your/predictions.txt \
    --model_name_or_path /path/to/your/huggingface_model \
    --db_base_dir spider_data/test_database \
    --sql_dialect "PostgreSQL" \
    --gpu "0" \
    --batch_size 32
```
You can change PostgreSQL to MySQL, SQLite, Oracle SQL, SQL Server, DuckDB, etc. 

### 3. Clean Inference Output
The output from the LLM (e.g., `predictions.txt`) might contain extra text, newlines, or formatting artifacts. The `clean.py` script standardizes the output to the required `index<TAB>SQL<TAB>db_id` format, merges multi-line queries, and extracts the first valid `SELECT` statement.
```bash
python clean.py \
    --input_file /path/to/your/predictions.txt \
    --output_file /path/to/your/cleaned_predictions.txt
```
### 4. Execute SQL Queries
After cleaning the predictions, you can execute the generated SQL queries against the databases. We provide separate scripts for different SQL dialects.

Note: The `run_sqlite.py` script works out-of-the-box with the provided `.sqlite` database files (assuming you have them). All other dialects require specific environment setup (see section below).

#### SQLite (Default):
```bash
python run_sqlite.py \
    --db_dir /path/to/spider_data/test_database \
    --input_file /path/to/your/cleaned_predictions.txt \
    --output_file /path/to/your/sqlite_results.txt
```

#### Other Dialects (e.g., MySQL):
For other dialects, you must provide connection details. See the "Advanced Environment Setup" section for prerequisites.
```bash
# Example for MySQL
python run_mysql.py \
    --mysql_socket  /path/to/your/mysql.sock file \
    --mysql_user "your_user" \
    --mysql_password "your_password" \
    --input_file /path/to/your/cleaned_predictions.txt \
    --output_file /path/to/your/mysql_results.txt
```
Other available execution scripts include:
- `run_postgres.py`
- `run_duckdb.py`
- `sqlite2sqlserver.py` and `run_sqlserver.py`
- `run_duckdb.py`
- `run_oracle.py`

### 5. Evaluation
Finally, evaluate the execution results (.txt file from Step 4) against the gold-standard solution file using eval.py.
```bash
python eval.py \
    --gold_result spider_data/test_sqlite_result.txt \
    --pred_result /path/to/your/sqlite_results.txt
```

## ⚙️ Advanced Environment Setup (coming soon)
To execute queries for dialects other than SQLite, you must install the necessary drivers and set up the databases.
### MySQL Setup

### PostgreSQL Setup 

### Microsoft SQL Server Setup

### Oracle Setup

### DuckDB Setup
