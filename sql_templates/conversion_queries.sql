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


-- ============================================================================
-- ERROR HANDLING & RETRY
-- ============================================================================

-- Identify failed conversions
SELECT 
    script_name,
    chunk_id,
    CASE 
        WHEN converted_code IS NULL THEN 'NULL_RESPONSE'
        WHEN converted_code LIKE '%error%' THEN 'ERROR_IN_OUTPUT'
        WHEN LENGTH(converted_code) < 100 THEN 'SUSPICIOUSLY_SHORT'
        ELSE 'OK'
    END AS status,
    LENGTH(source_code) AS source_length,
    LENGTH(converted_code) AS output_length
FROM {catalog}.{schema}.converted_chunks
WHERE converted_code IS NULL 
   OR converted_code LIKE '%error%'
   OR LENGTH(converted_code) < 100;

-- Retry failed chunks with different model
CREATE OR REPLACE TABLE {catalog}.{schema}.retry_conversions AS
SELECT 
    script_name,
    chunk_id,
    source_code,
    ai_query(
        '{alternate_model}',  -- Use different model for retry
        '{prompt}' || source_code,
        modelParameters => named_struct('max_tokens', 65000, 'temperature', 0.1)
    ) AS converted_code,
    '{alternate_model}' AS conversion_model,
    CURRENT_TIMESTAMP() AS converted_at
FROM {catalog}.{schema}.converted_chunks
WHERE converted_code IS NULL 
   OR LENGTH(converted_code) < 100;

-- Flag scripts needing manual review
ALTER TABLE {catalog}.{schema}.converted_chunks ADD COLUMN IF NOT EXISTS needs_review BOOLEAN DEFAULT FALSE;

UPDATE {catalog}.{schema}.converted_chunks
SET needs_review = TRUE
WHERE script_name IN (
    SELECT DISTINCT script_name 
    FROM {catalog}.{schema}.validation_results 
    WHERE validation_result LIKE 'failure%'
);


-- ============================================================================
-- TOKEN USAGE & COST TRACKING
-- ============================================================================

-- Estimate token usage per script (1 token ≈ 4 characters)
SELECT 
    script_name,
    SUM(CAST(LENGTH(source_code) / 4 AS INT)) AS input_tokens,
    SUM(CAST(LENGTH(converted_code) / 4 AS INT)) AS output_tokens,
    SUM(CAST((LENGTH(source_code) + LENGTH(converted_code)) / 4 AS INT)) AS total_tokens
FROM {catalog}.{schema}.converted_chunks
WHERE converted_code IS NOT NULL
GROUP BY script_name
ORDER BY total_tokens DESC;

-- Daily conversion volume
SELECT 
    DATE(converted_at) AS conversion_date,
    COUNT(DISTINCT script_name) AS scripts_processed,
    COUNT(*) AS chunks_processed,
    SUM(CAST(LENGTH(source_code) / 4 AS INT)) AS total_input_tokens,
    SUM(CAST(LENGTH(converted_code) / 4 AS INT)) AS total_output_tokens
FROM {catalog}.{schema}.converted_chunks
WHERE converted_code IS NOT NULL
GROUP BY DATE(converted_at)
ORDER BY conversion_date DESC;


-- ============================================================================
-- DASHBOARD SUMMARY VIEW
-- ============================================================================

CREATE OR REPLACE VIEW {catalog}.{schema}.conversion_dashboard AS
SELECT 
    'Total Scripts' AS metric,
    CAST(COUNT(DISTINCT script_name) AS STRING) AS value
FROM {catalog}.{schema}.input_scripts
UNION ALL
SELECT 
    'Converted Successfully',
    CAST(COUNT(DISTINCT script_name) AS STRING)
FROM {catalog}.{schema}.converted_chunks
WHERE converted_code IS NOT NULL AND LENGTH(converted_code) > 100
UNION ALL
SELECT 
    'Validation Pass Rate',
    CONCAT(CAST(ROUND(100.0 * SUM(CASE WHEN validation_result LIKE 'success%' THEN 1 ELSE 0 END) / COUNT(*), 1) AS STRING), '%')
FROM {catalog}.{schema}.validation_results
UNION ALL
SELECT 
    'Needs Review',
    CAST(COUNT(*) AS STRING)
FROM {catalog}.{schema}.converted_chunks
WHERE needs_review = TRUE
UNION ALL
SELECT 
    'Total Tokens Used',
    FORMAT_NUMBER(SUM(CAST((LENGTH(source_code) + LENGTH(converted_code)) / 4 AS BIGINT)), 0)
FROM {catalog}.{schema}.converted_chunks
WHERE converted_code IS NOT NULL;
