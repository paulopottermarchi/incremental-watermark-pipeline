# Databricks notebook source
# control/02_delta_maintenance
# ===========================================================================
# Weekly Delta housekeeping.
#
# Bronze is append-only and written once a day per source, so it accumulates
# small files indefinitely. Without compaction the marts get slower every
# week for reasons that have nothing to do with the data volume, and the
# cause is invisible from the SQL.
#
# OPTIMIZE compacts; ZORDER clusters on the key each downstream model
# filters or joins by; VACUUM reclaims storage from superseded files.
# ===========================================================================

# COMMAND ----------
dbutils.widgets.text("delta_base",     "dbfs:/mnt/penetration-pipeline")
dbutils.widgets.text("vacuum_hours",   "168")

delta_base   = dbutils.widgets.get("delta_base")
vacuum_hours = int(dbutils.widgets.get("vacuum_hours"))

MAINTENANCE_TARGETS = [
    {"path": f"{delta_base}/bronze/cases",           "zorder": "case_id"},
    {"path": f"{delta_base}/bronze/case_attributes", "zorder": "case_id"},
    {"path": f"{delta_base}/bronze/dialer",          "zorder": "case_id, call_date"},
    {"path": f"{delta_base}/bronze/contacts",        "zorder": "case_id, contact_date"},
]

# COMMAND ----------
for target in MAINTENANCE_TARGETS:
    path, zorder = target["path"], target["zorder"]
    print(f"[maintenance] OPTIMIZE {path} ZORDER BY ({zorder})")
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({zorder})")

# COMMAND ----------
# Retention must stay above the longest-running concurrent reader. One week
# is comfortable here: the longest job in this project is the daily run.
for target in MAINTENANCE_TARGETS:
    path = target["path"]
    print(f"[maintenance] VACUUM {path} RETAIN {vacuum_hours} HOURS")
    spark.sql(f"VACUUM delta.`{path}` RETAIN {vacuum_hours} HOURS")

print("Maintenance complete")
