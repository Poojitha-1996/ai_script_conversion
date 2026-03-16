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

> **Important**: Use a **different model** for validation than you used for conversion. This avoids self-bias - the same model may miss its own systematic errors or be lenient toward its own output patterns. For example: Convert with Claude → Validate with GPT-4 (or vice versa).

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

The following are examples - this approach works for **any text-based code that can be described in a prompt**:

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
| SAS | PySpark |
| Ab Initio | PySpark |
| DataStage | PySpark |
| Talend | PySpark |
| *Any SQL dialect* | *Spark SQL* |
| *Any ETL tool exports* | *PySpark* |

> **Note**: The LLM doesn't have hard-coded rules - it understands code from context. If you can describe the conversion in your prompt, it can convert it.

---

## Error Handling

### When Conversion Fails

```sql
-- Identify failed conversions (null or error responses)
SELECT 
    script_name,
    chunk_id,
    CASE 
        WHEN converted_code IS NULL THEN 'NULL_RESPONSE'
        WHEN converted_code LIKE '%error%' THEN 'ERROR_IN_OUTPUT'
        WHEN LENGTH(converted_code) < 100 THEN 'SUSPICIOUSLY_SHORT'
        ELSE 'OK'
    END AS status
FROM converted_scripts
WHERE converted_code IS NULL 
   OR converted_code LIKE '%error%'
   OR LENGTH(converted_code) < 100;
```

### Retry Pattern

```sql
-- Retry failed chunks with different model or adjusted prompt
CREATE OR REPLACE TABLE retry_conversions AS
SELECT 
    script_name,
    chunk_id,
    source_code,
    ai_query(
        'databricks-meta-llama-3-1-70b-instruct',  -- Try different model
        '{CONVERSION_PROMPT}' || source_code,
        modelParameters => named_struct('max_tokens', 65000, 'temperature', 0.1)  -- Slight temperature increase
    ) AS converted_code
FROM converted_scripts
WHERE converted_code IS NULL 
   OR LENGTH(converted_code) < 100;
```

### Manual Review Flag

```sql
-- Flag chunks that need human review
ALTER TABLE converted_scripts ADD COLUMN needs_review BOOLEAN;

UPDATE converted_scripts
SET needs_review = TRUE
WHERE script_name IN (
    SELECT script_name FROM validation_results 
    WHERE validation_result LIKE 'failure%'
);
```

---

## Cost Estimation

### Token Estimation

```python
# Rough estimation: 1 token ≈ 4 characters (for English/code)
def estimate_tokens(text):
    return len(text) / 4

# Calculate for your scripts
df = spark.table("input_scripts")
df_with_tokens = df.withColumn(
    "estimated_tokens",
    (F.length("source_code") / 4).cast("int")
)

# Total tokens for conversion
total_input_tokens = df_with_tokens.agg(F.sum("estimated_tokens")).collect()[0][0]

# Output is typically 1-1.5x input for code conversion
estimated_output_tokens = total_input_tokens * 1.25

print(f"Estimated input tokens: {total_input_tokens:,}")
print(f"Estimated output tokens: {estimated_output_tokens:,}")
print(f"Total tokens (input + output): {total_input_tokens + estimated_output_tokens:,}")
```

### Cost Factors

| Factor | Impact |
|--------|--------|
| Model choice | Claude/GPT-4 cost more than Llama |
| Script size | Larger scripts = more tokens |
| Chunk overlap | Some context may be repeated |
| Retries | Failed conversions cost extra |
| Validation | LLM-as-Judge doubles token usage |

### Cost Control Tips

- Start with smaller/cheaper models for initial testing
- Use chunking to avoid wasted tokens on failed large requests
- Skip validation for simple scripts
- Cache results - don't re-convert unchanged scripts

---

## Metrics & Monitoring

### Conversion Dashboard Queries

```sql
-- Overall conversion status
SELECT 
    source_type,
    COUNT(*) AS total_scripts,
    SUM(CASE WHEN converted_code IS NOT NULL THEN 1 ELSE 0 END) AS converted,
    SUM(CASE WHEN converted_code IS NULL THEN 1 ELSE 0 END) AS failed,
    ROUND(100.0 * SUM(CASE WHEN converted_code IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate
FROM converted_scripts
GROUP BY source_type;

-- Validation pass rate
SELECT 
    script_name,
    COUNT(*) AS total_chunks,
    SUM(CASE WHEN validation_result LIKE 'success%' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN validation_result LIKE 'failure%' THEN 1 ELSE 0 END) AS failed,
    ROUND(100.0 * SUM(CASE WHEN validation_result LIKE 'success%' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pass_rate
FROM validation_results
GROUP BY script_name
ORDER BY pass_rate ASC;

-- Scripts needing attention (failed validation)
SELECT 
    v.script_name,
    v.chunk_id,
    v.validation_result,
    SUBSTRING(c.source_code, 1, 200) AS source_preview
FROM validation_results v
JOIN converted_scripts c ON v.script_name = c.script_name AND v.chunk_id = c.chunk_id
WHERE v.validation_result LIKE 'failure%'
ORDER BY v.script_name, v.chunk_id;

-- Conversion timeline
SELECT 
    DATE(converted_at) AS conversion_date,
    COUNT(*) AS scripts_converted,
    SUM(LENGTH(source_code)) AS total_chars_processed
FROM converted_scripts
GROUP BY DATE(converted_at)
ORDER BY conversion_date;
```

### Token Usage Tracking

```sql
-- Track token usage per script (approximate)
SELECT 
    script_name,
    SUM(LENGTH(source_code) / 4) AS input_tokens,
    SUM(LENGTH(converted_code) / 4) AS output_tokens,
    SUM(LENGTH(source_code) / 4) + SUM(LENGTH(converted_code) / 4) AS total_tokens
FROM converted_scripts
GROUP BY script_name
ORDER BY total_tokens DESC;
```

---

## Notes

- **Chunking is use-case specific**: Adapt patterns based on source language
- **Prompt engineering is critical**: Invest time in detailed, accurate prompts
- **Temperature 0.0**: Ensures deterministic, reproducible conversions
- **Validation is optional but recommended**: LLM-as-Judge catches conversion gaps
- **Token limits**: Monitor chunk sizes to stay within model limits
