# AI-Powered Code Conversion IP

Convert any source code to PySpark using Databricks `ai_query()` - one SQL statement, no external tools.

## What It Does

- **Converts** legacy scripts (BTEQ, Informatica, PL/SQL, T-SQL, etc.) → PySpark/Spark SQL
- **Chunks** large scripts to handle LLM token limits - any script size works
- **Validates** output using LLM-as-Judge for functional parity
- **Scales** from 1 script to 1000 scripts with the same approach

## How It Works

```
Source Scripts → Chunking → ai_query(prompt + code) → Validation → Merged Output
```

## Core Concept

```sql
SELECT ai_query('model', 'conversion prompt' || source_code) AS converted_code
FROM source_table
```

That's it. The prompt defines the conversion. The LLM does the work.

## Quick Start

See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed implementation.

```sql
CREATE OR REPLACE TABLE converted_output AS
SELECT 
    script_name,
    source_code,
    ai_query(
        'databricks-claude-sonnet-4-5',
        '<your conversion prompt>' || source_code,
        modelParameters => named_struct('max_tokens', 65000, 'temperature', 0.0)
    ) AS converted_code
FROM input_scripts
```

## File Structure

```
code_conversion_ip/
├── README.md                 # This file
├── docs/
│   └── ARCHITECTURE.md       # Detailed implementation guide
├── prompts/                  # Example conversion prompts
│   ├── bteq_to_pyspark.md
│   └── llm_judge.md
└── sql_templates/            # Reusable SQL patterns
    └── conversion_queries.sql
```

## Key Points

- **Prompt = Configuration** - No complex setup, just write what you want converted
- **Chunking = Scalability** - Deterministic splitting enables any script size and avoids LLM token size     limitations
- **LLM-as-Judge = Quality** - Self-validating output without manual review
- **Delta Tables = Lineage** - Full audit trail of source, converted, and validation
