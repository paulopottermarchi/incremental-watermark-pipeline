# tests/test_jdbc_config.py
# ===========================================================================
# The watermark is interpolated into a SQL string because JDBC pushdown has
# no bind-parameter path. It comes from our own control table, but a control
# table is still an input — so the format guard is worth testing.
# ===========================================================================

import pytest

from utils.jdbc_config import _build_where, _validate_watermark


class TestWatermarkValidation:

    def test_accepts_well_formed_timestamp(self):
        assert _validate_watermark("2026-06-30 14:30:00") == "2026-06-30 14:30:00"

    @pytest.mark.parametrize(
        "value",
        [
            "2026-06-30",                      # no time component
            "2026-06-30T14:30:00",             # ISO 'T' separator
            "1900-01-01 00:00:00' OR '1'='1",  # injection attempt
            "",
        ],
    )
    def test_rejects_anything_else(self, value):
        with pytest.raises(ValueError):
            _validate_watermark(value)


class TestBuildWhere:

    def test_builds_predicate_without_extra_filter(self):
        assert _build_where("update_date", "2026-01-01 00:00:00", "") == \
            "WHERE update_date > '2026-01-01 00:00:00'"

    def test_appends_extra_filter(self):
        where = _build_where("update_date", "2026-01-01 00:00:00", "client_id IN (101)")
        assert where.endswith("AND client_id IN (101)")
