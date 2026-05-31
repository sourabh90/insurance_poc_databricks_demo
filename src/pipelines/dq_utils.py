import pyspark.sql.functions as F

QUARANTINE_COLS = ["source_table", "record_id", "dq_rule_violated", "raw_record", "quarantined_at"]


def add_dq(df, checks: list):
    """
    Appends a _dq column to df.
    checks: list of (spark_bool_expr, "issue_name") tuples.
    _dq is NULL when all checks pass; pipe-separated issue names when any fail.
    """
    flags = [F.when(expr, F.lit(name)) for expr, name in checks]
    raw = F.concat_ws("|", *flags)
    return df.withColumn("_dq", F.when(F.trim(raw) == "", F.lit(None)).otherwise(raw))


def to_quarantine(df, source_table: str, id_col: str):
    """Projects failed records (_dq IS NOT NULL) into the shared quarantine schema."""
    data_cols = [c for c in df.columns if c != "_dq"]
    return (
        df.filter(F.col("_dq").isNotNull())
        .withColumn("source_table",     F.lit(source_table))
        .withColumn("record_id",        F.col(id_col).cast("string"))
        .withColumn("dq_rule_violated", F.col("_dq"))
        .withColumn("raw_record",       F.to_json(F.struct(*data_cols)))
        .withColumn("quarantined_at",   F.current_timestamp())
        .select(*QUARANTINE_COLS)
    )
