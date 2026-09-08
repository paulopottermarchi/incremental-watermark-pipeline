# tests/test_watermark_utils.py
# ===========================================================================
# The watermark is the only piece of state this pipeline carries between
# runs. If it advances when it should not, rows are lost silently and the
# penetration number is quietly wrong — so the guarantees are worth pinning
# down: forward-only, unchanged on empty batches, unchanged on failure, and
# rewindable by a fixed lag for the transaction-commit overlap.
# ===========================================================================

from datetime import datetime

import pyspark.sql.functions as F
import pytest

# watermark_utils imports delta.tables at module level, so collection itself
# has to be conditional — otherwise an environment without delta-spark fails
# to collect this file rather than skipping it.
pytest.importorskip("delta", reason="delta-spark is not installed")

from utils.watermark_utils import (
    CONTROL_SCHEMA,
    get_watermark,
    mark_failed,
    update_watermark,
)


@pytest.fixture
def spark(delta_spark):
    """The watermark control table is Delta, so this module runs on that session."""
    return delta_spark


@pytest.fixture
def seeded_control(spark, tmp_path):
    control_path = str(tmp_path / "control" / "watermarks")
    df = spark.createDataFrame(
        [("dialer", datetime(2026, 1, 1, 0, 0, 0), 0, datetime.now(), "initialized")],
        schema=CONTROL_SCHEMA,
    )
    df.write.format("delta").mode("overwrite").save(control_path)
    return control_path


class TestGetWatermark:

    def test_returns_seed_for_unknown_table(self, spark, seeded_control):
        assert get_watermark(spark, seeded_control, "nonexistent") == "1900-01-01 00:00:00"

    def test_returns_seeded_watermark(self, spark, seeded_control):
        assert get_watermark(spark, seeded_control, "dialer") == "2026-01-01 00:00:00"

    def test_lag_rewinds_the_returned_watermark(self, spark, seeded_control):
        """The overlap re-read that covers late-committing transactions."""
        assert get_watermark(spark, seeded_control, "dialer", lag_minutes=15) == \
            "2025-12-31 23:45:00"

    def test_lag_never_rewinds_below_the_seed(self, spark, seeded_control):
        assert get_watermark(spark, seeded_control, "unknown", lag_minutes=60) == \
            "1900-01-01 00:00:00"


class TestUpdateWatermark:

    def test_empty_batch_does_not_advance(self, spark, seeded_control):
        update_watermark(spark, seeded_control, "dialer", None, 0)
        assert get_watermark(spark, seeded_control, "dialer") == "2026-01-01 00:00:00"

    def test_advances_to_batch_max(self, spark, seeded_control):
        update_watermark(
            spark, seeded_control, "dialer", datetime(2026, 6, 30, 14, 30, 0), 3
        )
        assert get_watermark(spark, seeded_control, "dialer") == "2026-06-30 14:30:00"

    def test_does_not_move_backward(self, spark, seeded_control):
        update_watermark(
            spark, seeded_control, "dialer", datetime(2026, 6, 30, 0, 0, 0), 1
        )
        update_watermark(
            spark, seeded_control, "dialer", datetime(2026, 3, 1, 0, 0, 0), 1
        )
        assert get_watermark(spark, seeded_control, "dialer") == "2026-06-30 00:00:00"

    def test_records_row_count(self, spark, seeded_control):
        update_watermark(
            spark, seeded_control, "dialer", datetime(2026, 6, 30, 0, 0, 0), 4_218
        )
        row = (
            spark.read.format("delta").load(seeded_control)
            .filter(F.col("table_name") == "dialer").collect()[0]
        )
        assert row["rows_processed"] == 4_218
        assert row["run_status"] == "success"


class TestMarkFailed:

    def test_failed_run_does_not_change_watermark(self, spark, seeded_control):
        mark_failed(spark, seeded_control, "dialer", "Connection timeout")
        assert get_watermark(spark, seeded_control, "dialer") == "2026-01-01 00:00:00"

    def test_failed_run_creates_row_for_unknown_table(self, spark, seeded_control):
        """
        Regression: this path used to build the control row from a string
        default while the column is a timestamp, so the merge blew up inside
        the error handler — losing the original exception with it.
        """
        mark_failed(spark, seeded_control, "new_table", "Source unreachable")

        row = (
            spark.read.format("delta").load(seeded_control)
            .filter(F.col("table_name") == "new_table").collect()
        )
        assert len(row) == 1
        assert "failed" in row[0]["run_status"]
        assert row[0]["last_watermark"] == datetime(1900, 1, 1, 0, 0, 0)
