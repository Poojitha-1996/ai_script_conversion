# Architecture & Implementation Guide

## Why LLM-Based Conversion?

### Advantages over Traditional Tools

- **No licensing costs** - Uses Databricks compute you already have; no separate tool licenses
- **No installation or configuration** - Works immediately via SQL; no agents, connectors, or infrastructure
- **Handles any source language** - Same approach works for BTEQ, Informatica, PL/SQL, T-SQL, etc. - just change the prompt
- **Adapts to edge cases** - LLMs understand context and intent; pattern-based tools fail on anything not explicitly programmed
- **Self-validating** - LLM-as-Judge can verify its own output; traditional tools require manual review
- **Prompt is the configuration** - No complex rule files; natural language instructions define the conversion
- **Continuously improving** - As LLM models improve, conversion quality improves with no code changes

### Advantages over Manual Conversion

- **Speed** - Converts in minutes what takes developers days or weeks
- **Consistency** - Same prompt produces consistent patterns across all scripts
- **Scalability** - Convert 1 script or 1000 scripts with the same SQL statement
- **No specialized expertise required** - Don't need deep knowledge of both source and target systems
- **Reduces human error** - No missed columns, forgotten joins, or copy-paste mistakes

### Considerations

- **Prompt engineering is critical** - Quality of output depends on quality of prompt
- **Validation still needed** - LLM output should be reviewed, especially for complex logic
- **Token limits** - Large scripts must be chunked (see below)
- **Not deterministic** - Same input may produce slightly different output (mitigated by temperature=0)

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CODE CONVERSION PIPELINE                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐  │
│  │  VOLUME  │──▶│ CHUNKING │──▶│  DELTA   │──▶│ AI_QUERY │──▶│  OUTPUT  │  │
│  │ (Scripts)│   │(Optional)│   │  TABLE   │   │(Convert) │   │  TABLE   │  │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘  │
│                                                                     │       │
│                                                                     ▼       │
│  ┌──────────┐                                              ┌──────────────┐ │
│  │  MERGED  │◀─────────────────────────────────────────────│  LLM JUDGE   │ │
│  │ NOTEBOOK │                                              │ (Validation) │ │
│  └──────────┘                                              └──────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Pipeline Steps

### Step 1: Chunking - Why It's Essential

**Problem**: LLMs have token limits. A model with 128K context window cannot process a 20,000-line script in one call.

**Solution**: Split large scripts into smaller chunks, convert each chunk, then merge the results.

**Key Insight**: With deterministic chunking, **any size script can be converted** - 1,000 lines or 100,000 lines - the approach is the same.

#### Why Deterministic Chunking?

- **Reproducible** - Same input always produces same chunks
- **Mergeable** - Chunks can be reassembled in correct order
- **Semantic** - Split at logical boundaries (functions, tables, sections) not arbitrary line counts
- **Dependency-aware** - Keep related code together (e.g., CREATE TABLE with its INSERT)

#### Chunking Strategies (Examples)

The chunking approach depends entirely on your source language:

| Source Type | Chunking Approach | Pattern Example |
|-------------|-------------------|-----------------|
| BTEQ | Label-based | `.label SECTION_NAME` |
| BTEQ | Marker-based | `/*--Temporary table--*/` |
| Informatica | XML element | `<TRANSFORMATION>` |
| PL/SQL | Procedure boundaries | `CREATE OR REPLACE PROCEDURE` |
| Generic | Line-based | Every N lines |
| Generic | Token-based | Split at ~50K tokens |

#### When to Skip Chunking

- Small scripts that fit within token limits
- Scripts with no clear logical boundaries
- When you want single-pass conversion for simplicity

#### Example: Simple Line-Based Chunking

```python
def chunk_by_lines(content: str, chunk_size: int = 1000) -> list:
    lines = content.split('\n')
    chunks = []
    for i in range(0, len(lines), chunk_size):
        chunk_content = '\n'.join(lines[i:i + chunk_size])
        chunks.append({
            'chunk_id': f"chunk_{i // chunk_size + 1:03d}",
            'content': chunk_content,
            'start_line': i + 1,
            'end_line': min(i + chunk_size, len(lines))
        })
    return chunks
```

#### Example: Pattern-Based Chunking

```python
import re

def chunk_by_pattern(content: str, pattern: str) -> list:
    """Split content at regex pattern matches"""
    chunks = []
    parts = re.split(f'({pattern})', content)
    
    current_chunk = ""
    chunk_id = 1
    
    for part in parts:
        if re.match(pattern, part):
            if current_chunk.strip():
                chunks.append({
                    'chunk_id': f"chunk_{chunk_id:03d}",
                    'content': current_chunk
                })
                chunk_id += 1
            current_chunk = part
        else:
            current_chunk += part
    
    if current_chunk.strip():
        chunks.append({
            'chunk_id': f"chunk_{chunk_id:03d}",
            'content': current_chunk
        })
    
    return chunks

# Usage examples:
# BTEQ labels: chunk_by_pattern(content, r'\.label\s+\w+')
# Temp tables: chunk_by_pattern(content, r'/\*--Temporary table--\*/')
# PL/SQL: chunk_by_pattern(content, r'CREATE\s+OR\s+REPLACE\s+PROCEDURE')
```

---

### Step 2: Load Scripts from Volume to Delta Table

Read source scripts from Unity Catalog Volume and load into Delta table.

```python
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row
import os

schema = StructType([
    StructField("script_name", StringType(), False),
    StructField("source_code", StringType(), False)
])

volume_path = "/Volumes/{catalog}/{schema}/{volume_name}"

files = dbutils.fs.ls(volume_path)
rows = []

for f in files:
    if f.path.endswith(".sql"):  # or .xml, .py, etc.
        content = dbutils.fs.head(f.path, 10000000)  # Adjust size as needed
        script_name = os.path.basename(f.path).replace('.sql', '')
        rows.append(Row(script_name=script_name, source_code=content))

df = spark.createDataFrame(rows, schema)
df.write.mode("overwrite").saveAsTable("{catalog}.{schema}.input_scripts")
```

---

### Step 3: AI Query Conversion

**The core of the IP** - single SQL statement converts code using LLM.

```sql
CREATE OR REPLACE TABLE {catalog}.{schema}.converted_scripts AS
SELECT 
    script_name,
    chunk_id,
    source_code,
    ai_query(
        'databricks-claude-sonnet-4-5',  -- or other model
        '{CONVERSION_PROMPT}' || source_code,
        modelParameters => named_struct(
            'max_tokens', 65000,
            'temperature', 0.0
        )
    ) AS converted_code
FROM {catalog}.{schema}.input_chunks
```

#### Prompt Engineering (Critical)

The conversion prompt is **the most important asset**. It must include:

1. **Task definition**: Clear conversion objective
2. **Constraints**: Data types, syntax mappings, naming conventions
3. **Examples**: Input → Output pairs for complex patterns
4. **Error handling**: How to handle edge cases
5. **Output format**: Code only, no explanations

See `prompts/` folder for source-specific prompt templates.

---

### Step 4: LLM-as-Judge Validation

Validate converted code using a second LLM call.

```sql
CREATE OR REPLACE TABLE {catalog}.{schema}.validation_results AS
SELECT 
    script_name,
    chunk_id,
    source_code,
    converted_code,
    ai_query(
        'databricks-claude-sonnet-4-5',
        '<validation prompt - see prompts/llm_judge.md>',
        modelParameters => named_struct('max_tokens', 60000, 'temperature', 0.0)
    ) AS validation_result
FROM {catalog}.{schema}.converted_scripts
```

---

### Step 5: Merge Chunks & Export

Reassemble chunks in correct order and export as notebooks.

```python
# Read converted chunks
df = spark.table("{catalog}.{schema}.converted_scripts")

# Order by script and chunk
df_ordered = df.orderBy("script_name", "chunk_id")

# Merge chunks per script
from pyspark.sql.functions import collect_list, concat_ws

merged_df = df_ordered.groupBy("script_name").agg(
    concat_ws("\n\n", collect_list("converted_code")).alias("full_converted_code")
)

# Export to notebooks (using Databricks Repos API or workspace API)
```

---

## Delta Table Schema

### Input Table: `input_scripts`

| Column | Type | Description |
|--------|------|-------------|
| script_name | STRING | Unique identifier for the script |
| source_code | STRING | Original source code |
| source_type | STRING | BTEQ, Informatica, PLSQL, etc. |
| file_path | STRING | Original volume path |
| loaded_at | TIMESTAMP | Load timestamp |

### Chunks Table: `input_chunks`

| Column | Type | Description |
|--------|------|-------------|
| script_name | STRING | Parent script name |
| chunk_id | STRING | Ordered chunk identifier (e.g., chunk_001) |
| chunk_sequence | INT | Numeric order for merging |
| source_code | STRING | Chunk content |
| start_line | INT | Starting line in original script |
| end_line | INT | Ending line in original script |

### Output Table: `converted_scripts`

| Column | Type | Description |
|--------|------|-------------|
| script_name | STRING | Script identifier |
| chunk_id | STRING | Chunk identifier |
| source_code | STRING | Original chunk code |
| converted_code | STRING | AI-converted PySpark code |
| conversion_model | STRING | Model used for conversion |
| converted_at | TIMESTAMP | Conversion timestamp |

### Validation Table: `validation_results`

| Column | Type | Description |
|--------|------|-------------|
| script_name | STRING | Script identifier |
| chunk_id | STRING | Chunk identifier |
| source_code | STRING | Original code |
| converted_code | STRING | Converted code |
| validation_result | STRING | Validation output (success/failure + explanation) |
| validated_at | TIMESTAMP | Validation timestamp |

---

## Supported Source Types

| Source | Target |
|--------|--------|
| Teradata BTEQ | PySpark/Spark SQL |
| Teradata Stored Procedures | PySpark |
| Informatica PowerCenter XML | PySpark |
| Oracle PL/SQL | PySpark |
| SQL Server T-SQL | PySpark |
| Snowflake SQL | PySpark |
| Hive/Impala | PySpark |
| AWS Glue (Python) | Databricks Notebooks |

---

## Notes

- **Chunking is use-case specific**: Adapt patterns based on source language
- **Prompt engineering is critical**: Invest time in detailed, accurate prompts
- **Temperature 0.0**: Ensures deterministic, reproducible conversions
- **Validation is optional but recommended**: LLM-as-Judge catches conversion gaps
- **Token limits**: Monitor chunk sizes to stay within model limits
