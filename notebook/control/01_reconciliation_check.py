# Databricks notebook source
# control/01_reconciliation_check
# ===========================================================================
# Weekly full-count reconciliation between source and Bronze.
#
# Watermark polling has two blind spots: rows that change without touching
# update_date, and rows committed after a read that stamped update_date at
# transaction start. The overlap re-read in utils/ingest.py covers the
# second. This check is what catches the first — before it surfaces as a
# wrong number in the penetration report.
# ===========================================================================

# COMMAND ----------
dbutils.widgets.text("delta_base",      "dbfs:/mnt/penetration-pipeline")
dbutils.widgets.text("repo_path",       "/Repos/data-eng/penetration-pipeline")
dbutils.widgets.text("drift_threshold", "0.005")
dbutils.widgets.text("client_ids",      "101, 102, 103")
dbutils.widgets.text("attr_type_ids",   "11, 12")

delta_base      = dbutils.widgets.get("delta_base")
repo_path       = dbutils.widgets.get("repo_path")
drift_threshold = float(dbutils.widgets.get("drift_threshold"))
client_ids      = dbutils.widgets.get("client_ids")
attr_type_ids   = dbutils.widgets.get("attr_type_ids")

import sys

sys.path.insert(0, repo_path)
import pyspark.sql.functions as F

from utils.jdbc_config import get_jdbc_properties, get_jdbc_url

BRONZE_BASE = f"{delta_base}/bronze"

RECONCILIATION_TARGETS = [
    {
        "name": "cases",
        "source_table": f"collections.[case] WHERE client_id IN ({client_ids})",
        "pk": "case_id",
    },
    {
        "name": "dialer",
        "source_table": f"collections.dialer WHERE client_id IN ({client_ids})",
        "pk": "uniqueid",
    },
    {
        "name": "case_attributes",
        "source_table": f"collections.case_attribute WHERE case_attribute_type_id IN ({attr_type_ids})",
        "pk": "case_id",
    },
    {
        "name": "contacts",
        "source_table": "collections.contact WHERE insert_user <> 'auto_dialer'",
        "pk": "contact_id",
    },
]

# COMMAND ----------
def get_source_count(table_with_filter: str) -> int:
    url, props = get_jdbc_url(), get_jdbc_properties()
    query = f"SELECT COUNT(*) AS cnt FROM {table_with_filter}"
    df = (
        spark.read.format("jdbc")
        .option("url", url).option("dbtable", f"({query}) AS q")
        .option("user", props["user"]).option("password", props["password"])
        .option("driver", props["driver"]).load()
    )
    return df.collect()[0]["cnt"]


def get_bronze_distinct_count(path: str, pk: str) -> int:
    return spark.read.format("delta").load(path).select(pk).distinct().count()

# COMMAND ----------
results = []
for target in RECONCILIATION_TARGETS:
    source_count = get_source_count(target["source_table"])
    bronze_count = get_bronze_distinct_count(f"{BRONZE_BASE}/{target['name']}", target["pk"])
    drift_pct    = round(abs(source_count - bronze_count) / source_count, 6) if source_count else 0
    flagged      = drift_pct > drift_threshold

    results.append({
        "table": target["name"], "source_count": source_count,
        "bronze_count": bronze_count, "drift_pct": drift_pct, "flagged": flagged,
    })
    status = "DRIFT" if flagged else "OK"
    print(f"[{target['name']}] source={source_count:,} bronze={bronze_count:,} drift={drift_pct:.4%} {status}")

# COMMAND ----------
recon_df = spark.createDataFrame(results).withColumn("checked_at", F.current_timestamp())
recon_df.write.format("delta").mode("append").save(f"{delta_base}/control/reconciliation_log")

flagged = [r["table"] for r in results if r["flagged"]]

# Fail the task on drift. Writing the finding to a log nobody reads is not
# a data quality check — the job has to go red for anyone to act on it.
if flagged:
    raise ValueError(f"Reconciliation drift above {drift_threshold:.2%} in: {', '.join(flagged)}")

print("All tables within threshold")
