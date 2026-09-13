# Databricks notebook source
# control/00_watermark_control_setup
# ===========================================================================
# Initializes the watermark control table. Run once.
# ===========================================================================

# COMMAND ----------
dbutils.widgets.text("delta_base", "dbfs:/mnt/penetration-pipeline")
dbutils.widgets.text("repo_path",  "/Repos/data-eng/penetration-pipeline")

delta_base = dbutils.widgets.get("delta_base")
repo_path  = dbutils.widgets.get("repo_path")

import sys

sys.path.insert(0, repo_path)
from datetime import datetime

from utils.watermark_utils import CONTROL_SCHEMA, SEED_WATERMARK

CONTROL_PATH = f"{delta_base}/control/watermarks"

# COMMAND ----------
# The control table shares its schema definition with watermark_utils, so a
# column added there cannot drift away from what is seeded here.
try:
    dbutils.fs.ls(CONTROL_PATH)
    print(f"[watermark_control] Already exists at {CONTROL_PATH}")
except Exception:
    seed_tables = ["cases", "case_attributes", "dialer", "contacts"]

    seed_df = spark.createDataFrame(
        [(t, SEED_WATERMARK, 0, datetime.now(), "initialized") for t in seed_tables],
        schema=CONTROL_SCHEMA,
    )

    seed_df.write.format("delta").mode("overwrite").save(CONTROL_PATH)
    print(f"[watermark_control] Initialized with {len(seed_tables)} sources")

# COMMAND ----------
display(spark.read.format("delta").load(CONTROL_PATH))
