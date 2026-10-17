"""
local/seed/generate_data.py
===========================================================================
Generates synthetic source data and writes it into the local SQL Server.

Uses Spark's own JDBC writer rather than a separate database client, so the
demo needs exactly one driver — the same one the pipeline itself loads. It
also means the seed step exercises the JDBC connectivity before the pipeline
does, which makes a misconfigured container fail here, with a clear message,
instead of three steps later inside an ingestion notebook.

Volumes are deliberately modest by default: large enough that the
incremental-versus-full-reload difference is visible on a laptop, small
enough that `make demo` finishes while you are still watching it.

    DEMO_CASES=60000 DEMO_CALLS=600000 python local/seed/generate_data.py
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from local.spark_session import build_local_spark, jdbc_properties, jdbc_url

# Illustrative business codes — these mirror the dbt vars in dbt_project.yml.
CLIENT_IDS = [101, 102, 103]
ATTR_PROVIDER, ATTR_DPD = 11, 12
DISPOSITIONS = [201, 202, 203]          # answered / no answer / failed
PROVIDERS = ["Provider Alpha", "Provider Beta", "Provider Gamma", "Provider Delta"]

N_CASES = int(os.environ.get("DEMO_CASES", 60_000))
N_CALLS = int(os.environ.get("DEMO_CALLS", 600_000))
N_CONTACTS = int(os.environ.get("DEMO_CONTACTS", 90_000))

# History depth. The reporting window is the last three months; generating
# six means the marts have to actually apply their window filter rather than
# trivially selecting everything.
HISTORY_DAYS = 180
CALL_WINDOW_DAYS = 120

SEED = 42


def _write(df, table: str) -> None:
    rows = df.count()
    print(f"  -> {table}: writing {rows:,} rows", flush=True)
    (
        df.write.format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", table)
        .option("user", jdbc_properties()["user"])
        .option("password", jdbc_properties()["password"])
        .option("driver", jdbc_properties()["driver"])
        .option("batchsize", "10000")
        .mode("append")
        .save()
    )


def build_cases(spark):
    return (
        spark.range(1, N_CASES + 1)
        .withColumnRenamed("id", "case_id")
        .withColumn("client_id",
                    F.element_at(F.array(*[F.lit(c) for c in CLIENT_IDS]),
                                 (F.col("case_id") % len(CLIENT_IDS)).cast("int") + 1))
        .withColumn("ref_number", F.concat(F.lit("REF-"), F.col("case_id").cast("string")))
        .withColumn("debtor_id", (F.col("case_id") * 7 % 250_000).cast("int"))
        .withColumn("original_capital",
                    F.round(F.rand(SEED) * 40_000 + 500, 2).cast("decimal(18,2)"))
        .withColumn("actual_capital",
                    F.round(F.col("original_capital") * (F.rand(SEED + 1) * 0.5 + 0.5), 2)
                     .cast("decimal(18,2)"))
        # ~8% closed, so stg_cases has something to filter out
        .withColumn("case_statute_id", F.when(F.rand(SEED + 2) < 0.08, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("update_date",
                    F.expr(f"current_timestamp() - make_interval(0,0,0,0,0,0,rand({SEED + 3}) * {HISTORY_DAYS * 86400})"))
        .select("case_id", "client_id", "ref_number", "debtor_id",
                "original_capital", "actual_capital", "case_statute_id", "update_date")
    )


def build_case_attributes(spark):
    provider_array = F.array(*[F.lit(p) for p in PROVIDERS])

    provider_rows = (
        spark.range(1, N_CASES + 1)
        .withColumnRenamed("id", "case_id")
        .withColumn("case_attribute_type_id", F.lit(ATTR_PROVIDER))
        .withColumn("case_attribute_value",
                    F.element_at(provider_array,
                                 (F.col("case_id") % len(PROVIDERS)).cast("int") + 1))
    )

    # DPD spread across every band, including a slice of nulls and a few
    # negatives (not yet due) so the 'Sem DPD' bucket is exercised.
    dpd_rows = (
        spark.range(1, N_CASES + 1)
        .withColumnRenamed("id", "case_id")
        .withColumn("case_attribute_type_id", F.lit(ATTR_DPD))
        .withColumn("r", F.rand(SEED + 4))
        .withColumn("case_attribute_value",
                    F.when(F.col("r") < 0.03, F.lit(None).cast("string"))
                     .when(F.col("r") < 0.05, F.lit("-5"))
                     .otherwise(F.floor(F.rand(SEED + 5) * 500).cast("string")))
        .drop("r")
    )

    return (
        provider_rows.unionByName(dpd_rows)
        .withColumn("update_date",
                    F.expr(f"current_timestamp() - make_interval(0,0,0,0,0,0,rand({SEED + 6}) * {HISTORY_DAYS * 86400})"))
        .select("case_id", "case_attribute_type_id", "case_attribute_value", "update_date")
    )


def build_dialer(spark):
    disposition_array = F.array(*[F.lit(d) for d in DISPOSITIONS])

    return (
        spark.range(N_CALLS)
        .withColumnRenamed("id", "seq")
        .withColumn("case_id", (F.floor(F.rand(SEED + 7) * N_CASES) + 1).cast("int"))
        .withColumn("uniqueid", F.concat(F.lit("call-"), F.col("seq").cast("string")))
        .withColumn("client_id",
                    F.element_at(F.array(*[F.lit(c) for c in CLIENT_IDS]),
                                 (F.col("case_id") % len(CLIENT_IDS)).cast("int") + 1))
        # A small pool of telecom_ids, skewed, so some of them clear the
        # cleanup threshold in mart_phone_quality and most do not.
        .withColumn("telecom_id", (F.floor(F.pow(F.rand(SEED + 8), 2) * 40) + 1).cast("int"))
        .withColumn("phone_number",
                    F.concat(F.lit("+5511"), F.lpad((F.col("case_id") % 100_000_000).cast("string"), 9, "0")))
        .withColumn("disposition_code",
                    F.element_at(disposition_array,
                                 (F.floor(F.rand(SEED + 9) * len(DISPOSITIONS)) + 1).cast("int")))
        .withColumn("call_date",
                    F.expr(f"current_timestamp() - make_interval(0,0,0,0,0,0,rand({SEED + 10}) * {CALL_WINDOW_DAYS * 86400})"))
        .withColumn("update_date", F.col("call_date"))
        .select("uniqueid", "case_id", "client_id", "telecom_id", "phone_number",
                "disposition_code", "call_date", "update_date")
    )


def build_contacts(spark):
    return (
        spark.range(1, N_CONTACTS + 1)
        .withColumnRenamed("id", "contact_id")
        .withColumn("case_id", (F.floor(F.rand(SEED + 11) * N_CASES) + 1).cast("int"))
        .withColumn("status_type_id", (F.floor(F.rand(SEED + 12) * 6) + 1).cast("int"))
        .withColumn("target_type_id", (F.floor(F.rand(SEED + 13) * 4) + 1).cast("int"))
        .withColumn("contact_date",
                    F.expr(f"current_timestamp() - make_interval(0,0,0,0,0,0,rand({SEED + 14}) * {CALL_WINDOW_DAYS * 86400})"))
        # ~35% system-generated, which stg_contacts must exclude
        .withColumn("insert_user",
                    F.when(F.rand(SEED + 15) < 0.35, F.lit("auto_dialer"))
                     .otherwise(F.concat(F.lit("operator_"),
                                         (F.floor(F.rand(SEED + 16) * 25) + 1).cast("string"))))
        .withColumn("update_date", F.col("contact_date"))
        .select("contact_id", "case_id", "status_type_id", "target_type_id",
                "contact_date", "insert_user", "update_date")
    )


def main() -> None:
    spark = build_local_spark("penetration-pipeline-seed")
    spark.sparkContext.setLogLevel("WARN")

    print("Seeding synthetic source data into SQL Server...", flush=True)
    _write(build_cases(spark), "collections.[case]")
    _write(build_case_attributes(spark), "collections.case_attribute")
    _write(build_dialer(spark), "collections.dialer")
    _write(build_contacts(spark), "collections.contact")

    print("Seed complete.", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
