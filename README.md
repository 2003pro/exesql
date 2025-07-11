# Exesql: Self-Taught Text-to-SQL Models with Execution-Driven Bootstrapping for SQL Dialects
This repository contains the official implementation of ExeSQL.


### Performance Comparison of LLMs on SQL Benchmarks

| Method               | Model size | PostgreSQL Spider | PostgreSQL WikiSQL | MySQL Spider | MySQL WikiSQL | MySQL Bird | Oracle Spider | Average |
|----------------------|------------|-------------------|--------------------|--------------|----------------|-------------|----------------|---------|

**_General Purposed LLM_**
| **GPT-4o**           | -          | 54.59             | 58.97              | 62.09        | 57.24          | 36.38       | 64.86          | 55.69   |
| **Gemini-1.5-pro**   | -          | 51.03             | 54.10              | 64.90        | 51.95          | 36.11       | 65.21          | 53.88   |
| **Llama3.1-Instruct**| 8B         | 33.63             | 31.60              | 48.86        | 25.41          | 24.58       | 30.00          | 32.35   |

**_Code Expert LLM_**
| **Deepseek-Coder**   | 7B         | 37.31             | 18.12              | 49.60        | 24.67          | 16.00       | 50.77          | 32.75   |
| **Qwen-Coder**       | 7B         | 36.80             | 15.48              | 39.04        | 22.84          | 15.36       | 58.31          | 31.31   |
| **Magicoder**        | 7B         | 21.90             | 17.45              | 47.28        | 23.32          | 13.23       | 26.60          | 24.96   |
| **WizardCoder**      | 15B        | 23.78             | 16.91              | 32.36        | 20.56          | 18.38       | 36.33          | 24.72   |

**_SQL Expert LLM_**
| **CodeS**            | 7B         | 24.76             | 20.00              | 35.60        | 23.00          | 14.41       | 37.40          | 25.86   |
| **StructLLM**        | 7B         | 38.71             | 30.97              | 44.20        | 7.14           | 22.69       | 33.16          | 29.48   |

**_Our Method_**
| **ExeSQL**           | 7B         | **69.86**         | **74.10**          | **72.09**    | **73.64**      | **41.13**   | **69.35**      | **66.70** |

> **Note**: *ExeSQL* outperforms all baseline models with an average improvement of 11.0% over GPT-4o.
