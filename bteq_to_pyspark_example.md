# Example: BTEQ to PySpark Conversion Prompt

> **Note**: This is an example prompt used for a specific BTEQ migration project. Your prompt will vary based on your source scripts, target requirements, and edge cases. Use this as inspiration - don't copy verbatim.

---

## Key Prompt Components

A good conversion prompt typically includes:

1. **Task** - What to convert
2. **Constraints** - Data types, syntax rules, naming conventions
3. **Mappings** - Source function → Target function
4. **Edge cases** - How to handle specific patterns
5. **Output format** - Code only, no explanations

---

## Example Prompt

```
Task:
Convert the following BTEQ logic into fully equivalent PySpark code.

Constraints:

Full Fidelity:

- Maintain complete equivalence with the original BTEQ logic.
- Every transformation, join condition, filter, and intermediate step must be preserved.
- Ensure that every column, alias, and derived expression in the PySpark/Spark SQL output matches exactly the BTEQ source: use the same names everywhere, do not rename, reorder, or substitute columns, and maintain consistency across all intermediate steps, joins, aliases and window functions.
- Convert all BTEQ comments to PySpark: change '--' to '#', convert '/* ... */' (single- or multi-line) to '#' lines, preserve content, formatting, decorative dashes, and move inline comments to separate lines above the statement.
- Convert UDF_GROUP_CONCAT(column, 'delimiter') OVER (...) to concat_ws('delimiter', collect_list(column) OVER (...)). Preserve the exact window specification (PARTITION BY, ORDER BY). Change CAST(... AS CHAR(n)) to CAST(... AS STRING).
- Convert every UDF to corresponding Pyspark code by replacing the UDF name with the corresponding PySpark function.
- Always preserve any QUALIFY clause exactly as written in the source. Keep the QUALIFY inside the same SELECT/subquery scope. Do not rewrite it as a JOIN filter or WHERE condition. For ROW_NUMBER()/RANK()/window functions, retain the full PARTITION BY and ORDER BY inside the QUALIFY expression 
- Replace all SQL-style variables like ${PARAM_*} with Python f-string variables {PARAM_*}
- Do not introduce aggregations or optimizations not present in the original.

Table Handling:

- Every CREATE TABLE statement in BTEQ should be translated into a physical table in PySpark.
- Do not create or drop temporary tables.

One-to-One Conversion:

- Every CREATE TABLE, INSERT INTO, REPLACE VIEW, DROP TABLE, and COLLECT STATISTICS statement must appear in the PySpark output.
- No original BTEQ line should remain unprocessed, untranslated, or omitted.
- Forward Alias Rule: Teradata allows using column aliases within the same SELECT (e.g., AS centroid then ST_X(centroid)), but PySpark doesn't. Fix by using a CTE to define the alias first, then reference it in the next SELECT, or repeat the full expression inline.

Geospatial Data Type Strategy:

- Always use STRING data type for all geometry columns
- Store geometries in WKT (Well-Known Text) format
- SYSUDTLIB.ST_GEOMETRY(size) INLINE LENGTH n → STRING (stores WKT format)
- SYSUDTLIB.MBR → STRING (stores envelope geometry as WKT)
- NEW ST_GEOMETRY(text_column, srid) → ST_AsText(ST_SetSRID(ST_GeomFromText(text_column), srid))
- geom_var.ST_ENVELOPE() → ST_Envelope(geom_var)
- geom_var.ST_CENTROID() → ST_Centroid(geom_var)
- point_var.ST_X() → ST_X(point_var)
- point_var.ST_Y() → ST_Y(point_var)

Index and Statistics:

- Replace PRIMARY INDEX with CLUSTER BY using the same columns
- PRIMARY INDEX (col1, col2, col3) → CLUSTER BY (col1, col2, col3)
- Whenever a BTEQ table column has a DEFAULT value, apply that default using COALESCE during INSERT INTO ... SELECT

Flow Control Conversion:

- Convert .GOTO LABEL_NAME (not part of Error check) into a Python function call: LABEL_NAME()
- Convert .LABEL LABEL_NAME into a Python function definition: def LABEL_NAME():
- Convert .IF ... THEN .GOTO LABEL_NAME into Python if statement
- For every BTEQ statement followed by .IF errorcode <> 0 THEN .GOTO LABEL, wrap SQL in try/except block
- For nested job lines (.OS tsl -nested ...), only retain as comment

Schema Mapping:

- Destination catalog: d0_azedh_bronze_dft
- Map BTEQ database names to schemas under d0_azedh_bronze_dft
- Replace ${...} schema variables with curated_avsre

Style Guide:

- Prefer Spark SQL where possible 
- Ensure every spark.sql() block is prefixed with f""" (f-string)
- Use DataFrame API only when necessary

Output:

- Output only the PySpark/Spark SQL code.
- Do not include any introduction or explanation.
```

---

## Usage

```sql
SELECT 
    script_name,
    source_code,
    ai_query(
        'databricks-claude-sonnet-4-5',
        '<your prompt>' || source_code,
        modelParameters => named_struct('max_tokens', 65000, 'temperature', 0.0)
    ) AS converted_code
FROM input_table
```

---

## Create Your Own

When building your conversion prompt, consider:

| Aspect | Questions to Answer |
|--------|---------------------|
| Source specifics | What functions, syntax, UDFs are unique to your scripts? |
| Target requirements | Which catalog/schema? Spark SQL or DataFrame API? |
| Edge cases | Geospatial? Flow control? Error handling? |
| Naming conventions | How should tables, columns, variables be named? |
| Comments | Preserve, convert, or ignore? |

The best prompt comes from iterating: run conversion → review output → refine prompt → repeat.
