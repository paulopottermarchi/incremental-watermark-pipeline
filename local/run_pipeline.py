"""
local/run_pipeline.py
===========================================================================
Runs the Bronze ingestion locally.

This file is a thin substitute for the Databricks job definition and nothing
more. It calls the same `utils.ingest.ingest_source` the notebooks call,
with the same arguments, so what runs here is the code that ships — not a
local reimplementation that could drift away from it and quietly stop
proving anything.

Everything the notebooks read from widgets is read from the environment
instead. Everything else is identical.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from datetime import datetime

from local.spark_session import DELTA_BASE, build_local_spark
from utils.ingest import ingest_source
from utils.watermark_utils import CONTROL_SCHEMA, SEED_WATERMARK

CLIENT_IDS = os.environ.get("DEMO_CLIENT_IDS", "101, 102, 103")
ATTR_TYPE_IDS = os.environ.get("DEMO_ATTR_TYPE_IDS", "11, 12")

SOURCES = [
    dict(
        table_name="cases",
        source_table="collections.[case]",
        select_columns="""
            case_id, client_id, ref_number, debtor_id,
            original_capital, actual_capital, case_statute_id, update_date
        """,
        extra_filter=f"client_id IN ({CLIENT_IDS})",
        partition_column="case_id",
        num_partitions=8,
    ),
    dict(
        table_name="case_attributes",
        source_table="collections.case_attribute",
        select_columns="""
            case_id, case_attribute_type_id, case_attribute_value, update_date
        """,
        extra_filter=f"case_attribute_type_id IN ({ATTR_TYPE_IDS})",
        partition_column="case_id",
        num_partitions=8,
    ),
    dict(
        table_name="dialer",
        source_table="collections.dialer",
        select_columns="""
            uniqueid, case_id, client_id, telecom_id, phone_number,
            disposition_code, call_date, update_date
        """,
        extra_filter=f"client_id IN ({CLIENT_IDS})",
        partition_column="case_id",
        num_partitions=16,
    ),
    dict(
        table_name="contacts",
        source_table="collections.contact",
        select_columns="""
            contact_id, case_id, status_type_id, target_type_id,
            contact_date, insert_user, update_date
        """,
        partition_column="contact_id",
        num_partitions=8,
    ),
]


def ensure_control_table(spark) -> None:
    """Local equivalent of notebooks/control/00_watermark_control_setup."""
    control_path = f"{DELTA_BASE}/control/watermarks"

    try:
        spark.read.format("delta").load(control_path)
        print(f"[control] Watermark table already exists at {control_path}")
        return
    except Exception:
        pass

    seed_tables = [s["table_name"] for s in SOURCES]
    (
        spark.createDataFrame(
            [(t, SEED_WATERMARK, 0, datetime.now(), "initialized") for t in seed_tables],
            schema=CONTROL_SCHEMA,
        )
        .write.format("delta").mode("overwrite").save(control_path)
    )
    print(f"[control] Initialized watermark table with {len(seed_tables)} sources")


def main() -> None:
    spark = build_local_spark("penetration-pipeline-ingest")
    spark.sparkContext.setLogLevel("WARN")

    ensure_control_table(spark)

    total = 0
    for source in SOURCES:
        total += ingest_source(spark, delta_base=DELTA_BASE, **source)

    print(f"\nIngestion complete: {total:,} rows landed in Bronze.")
    print("Registered tables:")
    spark.sql("SHOW TABLES IN bronze").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
