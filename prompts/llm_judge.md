# LLM-as-Judge Validation Prompt

Validate functional parity between source and converted code using a second LLM call.

---

## Concept

Use `ai_query()` to have an LLM compare source and converted code, checking for functional equivalence. The prompt should instruct the LLM to:

1. Compare statement by statement
2. Focus on **functional equivalence** (not syntax)
3. Provide evidence for each decision
4. Return a clear pass/fail result

---

## Example Prompt (BTEQ → PySpark)

This is one example - adapt for your source language and validation requirements.

```
Task:
Goal: Validate functional parity between "{source_language} source" and "PySpark target"

Domain Invariants (MUST HOLD):
- Every SELECT, JOIN, FILTER, CASE, transformation in source must exist in PySpark
- Intermediate steps (temp tables, derived columns) must be equivalent
- Column names and expressions must match in logic (syntax differences allowed)
- No missing steps, no extra transformations that change outputs

Comparison Method:
- Proceed statement by statement
- For every source statement, show corresponding PySpark statement
- If mismatch, quote both sides and explain

Functional Equivalence Rules:
- Date functions producing same result = MATCH
- Syntax differences with same output = MATCH
- Error handling differences = IGNORE
- Index/partitioning differences = IGNORE (Spark doesn't support same constructs)

Evidence Requirement:
- Include snippet from both sides for each decision
- Only flag TRUE MISMATCH if conversion produces DIFFERENT RESULTS

Artifact to be compared:
source->{source_code} 
target->{converted_code}

Output:
First token must be "success" or "failure", followed by explanation.
```

---

## Example Usage

```sql
SELECT 
    script_name,
    ai_query(
        'databricks-claude-sonnet-4-5',
        '<your validation prompt>' || ' source->' || source_code || ' target->' || converted_code,
        modelParameters => named_struct('max_tokens', 60000, 'temperature', 0.0)
    ) AS validation_result
FROM converted_scripts
```

---

## Design Your Own

Key decisions when creating your validation prompt:

| Decision | Options |
|----------|---------|
| Output format | Binary (success/failure), JSON scores, detailed report |
| Strictness | Functional only, syntax + functional, exact match |
| Evidence | Required snippets, optional, summary only |
| Exceptions | What differences to ignore (error handling, comments, etc.) |
| Token limit | Based on script size (e.g., 2K for short, 60K for long) |
