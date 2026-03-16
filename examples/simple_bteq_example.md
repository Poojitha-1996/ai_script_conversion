# Working Example: Simple BTEQ to PySpark

This example demonstrates the end-to-end conversion process.

---

## Sample Input (BTEQ)

```sql
/*--Temporary table--*/
CREATE VOLATILE TABLE temp_customers AS (
    SELECT 
        customer_id,
        customer_name,
        TRIM(email) AS email_clean,
        CAST(registration_date AS DATE) AS reg_date
    FROM ${DBEUDS_BCC_WR}.CUSTOMERS
    WHERE status = 'ACTIVE'
      AND registration_date > DATE - 365
) WITH DATA PRIMARY INDEX (customer_id)
ON COMMIT PRESERVE ROWS;

/*--Result table--*/
CREATE TABLE ${DBEUDS_BCC_WR}.CUSTOMER_SUMMARY AS (
    SELECT 
        customer_id,
        customer_name,
        email_clean,
        reg_date,
        CURRENT_DATE AS load_date
    FROM temp_customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY reg_date DESC) = 1
) WITH DATA PRIMARY INDEX (customer_id);

COLLECT STATISTICS ON ${DBEUDS_BCC_WR}.CUSTOMER_SUMMARY;
```

---

## Expected Output (PySpark)

```python
# --Temporary table--
spark.sql(f"""
CREATE OR REPLACE TABLE temp_customers AS
SELECT 
    customer_id,
    customer_name,
    TRIM(email) AS email_clean,
    CAST(registration_date AS DATE) AS reg_date
FROM d0_azedh_bronze_dft.curated_avsre.CUSTOMERS
WHERE status = 'ACTIVE'
  AND registration_date > DATE_SUB(CURRENT_DATE(), 365)
""")

# --Result table--
spark.sql(f"""
CREATE OR REPLACE TABLE d0_azedh_bronze_dft.curated_avsre.CUSTOMER_SUMMARY
CLUSTER BY (customer_id)
AS
SELECT 
    customer_id,
    customer_name,
    email_clean,
    reg_date,
    CURRENT_DATE() AS load_date
FROM temp_customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY reg_date DESC) = 1
""")

# COLLECT STATISTICS equivalent
spark.sql("ANALYZE TABLE d0_azedh_bronze_dft.curated_avsre.CUSTOMER_SUMMARY COMPUTE STATISTICS")
```

---

## Key Conversions Demonstrated

| BTEQ | PySpark |
|------|---------|
| `CREATE VOLATILE TABLE` | `CREATE OR REPLACE TABLE` |
| `${DBEUDS_BCC_WR}` | `d0_azedh_bronze_dft.curated_avsre` |
| `DATE - 365` | `DATE_SUB(CURRENT_DATE(), 365)` |
| `PRIMARY INDEX (col)` | `CLUSTER BY (col)` |
| `WITH DATA` | (implicit in CREATE AS) |
| `QUALIFY ROW_NUMBER()` | `QUALIFY ROW_NUMBER()` (supported in Databricks) |
| `COLLECT STATISTICS` | `ANALYZE TABLE ... COMPUTE STATISTICS` |

---

## Running This Example

### Step 1: Load the sample
```sql
CREATE TABLE sample_input AS
SELECT 
    'sample_bteq_script' AS script_name,
    '<paste BTEQ code above>' AS source_code;
```

### Step 2: Convert
```sql
CREATE TABLE sample_output AS
SELECT 
    script_name,
    source_code,
    ai_query(
        'databricks-claude-sonnet-4-5',
        'Convert this BTEQ to PySpark. 
        Map ${DBEUDS_BCC_WR} to d0_azedh_bronze_dft.curated_avsre.
        Replace PRIMARY INDEX with CLUSTER BY.
        Convert COLLECT STATISTICS to ANALYZE TABLE.
        Output only code: ' || source_code,
        modelParameters => named_struct('max_tokens', 4000, 'temperature', 0.0)
    ) AS converted_code
FROM sample_input;
```

### Step 3: Review
```sql
SELECT converted_code FROM sample_output;
```

---

## Common Variations

### If QUALIFY doesn't work (older Spark versions)

Use subquery with window function instead:

```python
spark.sql(f"""
CREATE OR REPLACE TABLE d0_azedh_bronze_dft.curated_avsre.CUSTOMER_SUMMARY AS
SELECT customer_id, customer_name, email_clean, reg_date, load_date
FROM (
    SELECT 
        customer_id,
        customer_name,
        email_clean,
        reg_date,
        CURRENT_DATE() AS load_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY reg_date DESC) AS rn
    FROM temp_customers
)
WHERE rn = 1
""")
```

### If you prefer DataFrame API

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read source
customers_df = spark.table("d0_azedh_bronze_dft.curated_avsre.CUSTOMERS")

# Transform
temp_df = customers_df.filter(
    (F.col("status") == "ACTIVE") & 
    (F.col("registration_date") > F.date_sub(F.current_date(), 365))
).select(
    "customer_id",
    "customer_name",
    F.trim("email").alias("email_clean"),
    F.col("registration_date").cast("date").alias("reg_date")
)

# Deduplicate with window
window = Window.partitionBy("customer_id").orderBy(F.desc("reg_date"))
result_df = temp_df.withColumn("rn", F.row_number().over(window)) \
    .filter(F.col("rn") == 1) \
    .drop("rn") \
    .withColumn("load_date", F.current_date())

# Write
result_df.write.mode("overwrite").saveAsTable("d0_azedh_bronze_dft.curated_avsre.CUSTOMER_SUMMARY")
```
