# tests/test_mart_logic.py
# ===========================================================================
# Business-logic invariants for the two marts, expressed as standalone
# pytest assertions so the bucketing and rate logic can be checked without a
# full dbt run. The dbt schema tests cover the same invariants against real
# built tables.
# ===========================================================================

import pyspark.sql.functions as F


def dpd_band(dpd_col):
    """Mirrors the dpd_faixa CASE WHEN in mart_penetration_by_dpd_band.sql"""
    return (
        F.when(dpd_col.between(0, 30),    "A: 0-30")
         .when(dpd_col.between(31, 60),   "B: 31-60")
         .when(dpd_col.between(61, 90),   "C: 61-90")
         .when(dpd_col.between(91, 180),  "D: 91-180")
         .when(dpd_col.between(181, 360), "E: 181-360")
         .when(dpd_col > 360,             "F: 361+")
         .otherwise("Sem DPD")
    )


def penetration_rate():
    """
    Mirrors the penetration_rate expression in the mart.

    Written as expr() rather than F.nullif(): the DataFrame API's nullif
    does not accept a Column as its second argument on PySpark 3.5, so the
    obvious spelling raises AnalysisException at plan time. The mart is SQL,
    so expressing it as SQL also keeps the two in step.
    """
    return F.expr("round(acionado / nullif(quantidade, 0), 4)")


class TestDpdBanding:

    def test_boundary_values_assigned_correctly(self, spark):
        df = spark.createDataFrame(
            [(0,), (30,), (31,), (60,), (61,), (90,), (91,), (180,),
             (181,), (360,), (361,), (None,)],
            schema="dpd int",
        ).withColumn("dpd_faixa", dpd_band(F.col("dpd")))

        result = {row["dpd"]: row["dpd_faixa"] for row in df.collect()}

        assert result[0]    == "A: 0-30"
        assert result[30]   == "A: 0-30"
        assert result[31]   == "B: 31-60"
        assert result[60]   == "B: 31-60"
        assert result[61]   == "C: 61-90"
        assert result[90]   == "C: 61-90"
        assert result[91]   == "D: 91-180"
        assert result[180]  == "D: 91-180"
        assert result[181]  == "E: 181-360"
        assert result[360]  == "E: 181-360"
        assert result[361]  == "F: 361+"
        assert result[None] == "Sem DPD"

    def test_negative_dpd_falls_through_to_sem_dpd(self, spark):
        """
        A case not yet due can carry a negative DPD. No band covers it, so it
        lands in 'Sem DPD' rather than silently joining the 0-30 bucket and
        inflating the freshest segment.
        """
        df = spark.createDataFrame([(-5,)], schema="dpd int") \
                  .withColumn("dpd_faixa", dpd_band(F.col("dpd")))
        assert df.collect()[0]["dpd_faixa"] == "Sem DPD"


class TestPenetrationRateLogic:

    def test_penetration_rate_bounded(self, spark):
        df = spark.createDataFrame(
            [("A", 100, 40), ("B", 50, 50), ("C", 10, 0)],
            schema="segment string, quantidade int, acionado int",
        ).withColumn("penetration_rate", penetration_rate())

        rates = [row["penetration_rate"] for row in df.collect()]
        assert all(0 <= r <= 1 for r in rates)

    def test_zero_quantidade_yields_null_not_error(self, spark):
        """nullif guards the division; an empty segment must not fail the run."""
        df = spark.createDataFrame(
            [("empty", 0, 0)],
            schema="segment string, quantidade int, acionado int",
        ).withColumn("penetration_rate", penetration_rate())
        assert df.collect()[0]["penetration_rate"] is None

    def test_sem_ac_equals_quantidade_minus_acionado(self, spark):
        df = spark.createDataFrame(
            [("A", 100, 40), ("B", 50, 50)],
            schema="segment string, quantidade int, acionado int",
        ).withColumn("sem_ac", F.col("quantidade") - F.col("acionado"))

        for row in df.collect():
            assert row["sem_ac"] == row["quantidade"] - row["acionado"]


class TestContactStatsLogic:

    def test_paid_ptp_cc_are_independent_categories(self, spark):
        """
        Paid, PTP and right-party are independent boolean buckets — a single
        contact can count toward more than one, because they are separate
        COUNT(DISTINCT CASE WHEN ...) expressions, not branches of one CASE.
        """
        df = spark.createDataFrame(
            [
                ("c1", 1, 1),   # paid AND right-party -> counts in both
                ("c2", 2, 3),   # ptp only
                ("c3", 3, 2),   # ptp AND right-party
                ("c4", 5, 3),   # none
            ],
            schema="contact_id string, status_type_id int, target_type_id int",
        )

        paid = df.filter(F.col("status_type_id") == 1).select("contact_id").distinct().count()
        ptp  = df.filter(F.col("status_type_id").isin(2, 3)).select("contact_id").distinct().count()
        cc   = df.filter(F.col("target_type_id").isin(1, 2)).select("contact_id").distinct().count()

        assert paid == 1   # c1
        assert ptp  == 2   # c2, c3
        assert cc   == 2   # c1, c3

    def test_system_generated_contacts_excluded(self, spark):
        df = spark.createDataFrame(
            [("c1", "operator_joao"), ("c2", "auto_dialer"), ("c3", "operator_maria")],
            schema="contact_id string, insert_user string",
        )
        filtered = df.filter(F.col("insert_user") != "auto_dialer")

        assert filtered.count() == 2
        assert "c2" not in [row["contact_id"] for row in filtered.collect()]


class TestPhoneQualityLogic:

    def test_cleanup_candidate_matches_threshold(self, spark):
        df = spark.createDataFrame(
            [
                ("t1", 10, 8),   # total > 5, failed > 6 -> candidate
                ("t2", 5, 4),    # total not > 5
                ("t3", 8, 6),    # failed not > 6
                ("t4", 20, 15),  # candidate
            ],
            schema="telecom_id string, total_calls int, failed_calls int",
        ).withColumn(
            "is_cleanup_candidate",
            F.when((F.col("total_calls") > 5) & (F.col("failed_calls") > 6), 1).otherwise(0),
        )

        result = {row["telecom_id"]: row["is_cleanup_candidate"] for row in df.collect()}
        assert result["t1"] == 1
        assert result["t2"] == 0
        assert result["t3"] == 0
        assert result["t4"] == 1
