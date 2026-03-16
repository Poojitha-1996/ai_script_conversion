-- ============================================================================
-- AI-POWERED CODE CONVERSION SQL TEMPLATES
-- ============================================================================
-- Replace placeholders:
--   {catalog}        - Unity Catalog name
--   {schema}         - Schema name
--   {volume}         - Volume name
--   {model}          - Model endpoint (e.g., databricks-claude-sonnet-4-5)
--   {prompt}         - Conversion prompt from prompts/ folder
-- ============================================================================


-- ============================================================================
-- STEP 1: CREATE INPUT TABLE SCHEMA
-- ============================================================================

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.input_scripts (
    script_name STRING NOT NULL COMMENT 'Unique script identifier',
    source_code STRING NOT NULL COMMENT 'Original source code',
    source_type STRING COMMENT 'Source type: BTEQ, Informatica, PLSQL, etc.',
    file_path STRING COMMENT 'Original volume path',
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Source scripts for conversion';


-- ============================================================================
-- STEP 2: CREATE CHUNKS TABLE (IF USING CHUNKING)
-- ============================================================================

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.input_chunks (
    script_name STRING NOT NULL,
    chunk_id STRING NOT NULL COMMENT 'Ordered chunk ID (e.g., chunk_001)',
    chunk_sequence INT COMMENT 'Numeric order for merging',
    source_code STRING NOT NULL,
    start_line INT,
    end_line INT,
    chunked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Chunked source scripts';


-- ============================================================================
-- STEP 3: CONVERSION - FULL SCRIPTS (NO CHUNKING)
-- ============================================================================

CREATE OR REPLACE TABLE {catalog}.{schema}.converted_scripts AS
SELECT 
    script_name,
    source_code,
    ai_query(
        '{model}',
        '{prompt}' || source_code,
        modelParameters => named_struct(
            'max_tokens', 65000,
            'temperature', 0.0
        )
    ) AS converted_code,
    '{model}' AS conversion_model,
    CURRENT_TIMESTAMP() AS converted_at
FROM {catalog}.{schema}.input_scripts;


-- ============================================================================
-- STEP 3 (ALT): CONVERSION - WITH CHUNKING
-- ============================================================================

CREATE OR REPLACE TABLE {catalog}.{schema}.converted_chunks AS
SELECT 
    script_name,
    chunk_id,
    chunk_sequence,
    source_code,
    ai_query(
        '{model}',
        '{prompt}' || source_code,
        modelParameters => named_struct(
            'max_tokens', 65000,
            'temperature', 0.0
        )
    ) AS converted_code,
    '{model}' AS conversion_model,
    CURRENT_TIMESTAMP() AS converted_at
FROM {catalog}.{schema}.input_chunks;


-- ============================================================================
-- STEP 4: LLM-AS-JUDGE VALIDATION
-- ============================================================================

CREATE OR REPLACE TABLE {catalog}.{schema}.validation_results AS
SELECT 
    script_name,
    chunk_id,
    source_code,
    converted_code,
    ai_query(
        '{model}',
        'You are a code review expert. Compare source and converted code for functional equivalence.

SOURCE CODE:
' || source_code || '

CONVERTED CODE:
' || converted_code || '

Evaluate and return JSON only:
{
  "functional_parity": <0-100>,
  "completeness": <0-100>,
  "syntax_correctness": <0-100>,
  "issues": ["issue1", "issue2"],
  "overall_pass": <true/false>
}',
        modelParameters => named_struct('max_tokens', 2000, 'temperature', 0.0)
    ) AS validation_json,
    CURRENT_TIMESTAMP() AS validated_at
FROM {catalog}.{schema}.converted_chunks;


-- ============================================================================
-- STEP 5: PARSE VALIDATION RESULTS
-- ============================================================================

CREATE OR REPLACE TABLE {catalog}.{schema}.validation_summary AS
SELECT 
    script_name,
    chunk_id,
    validation_json,
    GET_JSON_OBJECT(validation_json, '$.functional_parity')::INT AS functional_parity,
    GET_JSON_OBJECT(validation_json, '$.completeness')::INT AS completeness,
    GET_JSON_OBJECT(validation_json, '$.syntax_correctness')::INT AS syntax_score,
    GET_JSON_OBJECT(validation_json, '$.overall_pass')::BOOLEAN AS overall_pass,
    GET_JSON_OBJECT(validation_json, '$.issues') AS issues
FROM {catalog}.{schema}.validation_results;


-- ============================================================================
-- STEP 6: MERGE CHUNKS INTO FINAL OUTPUT
-- ============================================================================

CREATE OR REPLACE TABLE {catalog}.{schema}.final_converted_scripts AS
SELECT 
    script_name,
    CONCAT_WS('\n\n', COLLECT_LIST(converted_code)) AS full_converted_code,
    COUNT(*) AS chunk_count,
    MIN(converted_at) AS conversion_started,
    MAX(converted_at) AS conversion_completed
FROM (
    SELECT * FROM {catalog}.{schema}.converted_chunks
    ORDER BY script_name, chunk_sequence
)
GROUP BY script_name;


-- ============================================================================
-- MONITORING QUERIES
-- ============================================================================

-- Conversion status summary
SELECT 
    source_type,
    COUNT(*) AS total_scripts,
    SUM(CASE WHEN converted_code IS NOT NULL THEN 1 ELSE 0 END) AS converted,
    SUM(CASE WHEN converted_code IS NULL THEN 1 ELSE 0 END) AS pending
FROM {catalog}.{schema}.converted_scripts
GROUP BY source_type;

-- Validation pass/fail summary
SELECT 
    script_name,
    COUNT(*) AS total_chunks,
    SUM(CASE WHEN overall_pass THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN NOT overall_pass THEN 1 ELSE 0 END) AS failed,
    AVG(functional_parity) AS avg_parity_score
FROM {catalog}.{schema}.validation_summary
GROUP BY script_name
ORDER BY avg_parity_score;

-- Failed chunks requiring review
SELECT 
    script_name,
    chunk_id,
    functional_parity,
    completeness,
    issues
FROM {catalog}.{schema}.validation_summary
WHERE NOT overall_pass
ORDER BY functional_parity;
