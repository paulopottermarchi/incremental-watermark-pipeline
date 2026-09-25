"""
local/benchmark.py
===========================================================================
Measures what this pipeline actually claims: that producing the penetration
report does not require re-reading the whole window every day.

Method
------
Both arms answer the same question — "what changed that the report needs?"
— after the same simulated day of activity (local/seed/02_churn.sql).

  full_reload   reads the entire reporting window from SQL Server, the way
                the original query did on every execution.

  incremental   reads only rows whose update_date is above the stored
                watermark, the way this pipeline does.

Reported: wall-clock seconds and rows read from the source. Rows read is the
number that transfers between machines; wall-clock on a laptop with a
containerised SQL Server is indicative only, and the output says so rather
than dressing it up.

What this does NOT measure: the marts. They rebuild fully in both arms. The
saving claimed here is on extraction, and the benchmark is scoped to that on
purpose — a benchmark that quietly credits the pipeline for work it did not
avoid is worse than no benchmark.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from local.spark_session import DELTA_BASE, build_local_spark, jdbc_properties, jdbc_url
from utils.watermark_utils import get_watermark

CLIENT_FILTER = "client_id IN (101, 102, 103)"
WINDOW_MONTHS = 3


def _read(spark, query: str):
    props = jdbc_properties()
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", f"({query}) AS q")
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .option("fetchsize", "10000")
        .load()
    )


def time_full_reload(spark) -> tuple:
    query = (
        "SELECT uniqueid, case_id, client_id, telecom_id, phone_number, "
        "       disposition_code, call_date, update_date "
        "FROM collections.dialer "
        f"WHERE {CLIENT_FILTER} "
        f"  AND call_date >= DATEADD(MONTH, -{WINDOW_MONTHS}, GETDATE())"
    )
    start = time.perf_counter()
    rows = _read(spark, query).count()
    return time.perf_counter() - start, rows


def time_incremental(spark) -> tuple:
    control_path = f"{DELTA_BASE}/control/watermarks"
    watermark = get_watermark(spark, control_path, "dialer", lag_minutes=15)

    query = (
        "SELECT uniqueid, case_id, client_id, telecom_id, phone_number, "
        "       disposition_code, call_date, update_date "
        "FROM collections.dialer "
        f"WHERE {CLIENT_FILTER} "
        f"  AND update_date > '{watermark}'"
    )
    start = time.perf_counter()
    rows = _read(spark, query).count()
    return time.perf_counter() - start, rows


def main() -> None:
    spark = build_local_spark("penetration-pipeline-benchmark")
    spark.sparkContext.setLogLevel("ERROR")

    # A warm-up read so neither arm pays for connection setup and cold cache.
    _read(spark, "SELECT TOP 1 case_id FROM collections.dialer").count()

    full_secs, full_rows = time_full_reload(spark)
    inc_secs, inc_rows = time_incremental(spark)

    print("\n" + "=" * 62)
    print("  Extraction cost — same question, after one day of activity")
    print("=" * 62)
    print(f"  {'':<14}{'rows read':>14}{'seconds':>12}")
    print(f"  {'full reload':<14}{full_rows:>14,}{full_secs:>12.2f}")
    print(f"  {'incremental':<14}{inc_rows:>14,}{inc_secs:>12.2f}")
    print("-" * 62)

    if full_rows:
        print(f"  rows read:   {inc_rows / full_rows:.2%} of the full reload")
    if inc_secs > 0:
        print(f"  wall clock:  {full_secs / inc_secs:.1f}x faster")

    print("\n  Wall clock on a containerised SQL Server is indicative only.")
    print("  Rows read is the number that transfers between machines.")
    print("=" * 62 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
