-- local/seed/02_churn.sql
-- ===========================================================================
-- Simulates one day of activity in the source system: a slice of rows gets
-- a new update_date, as if they had been touched by the collection floor.
--
-- This is what makes the benchmark honest. Comparing a full reload against
-- an incremental pull on a static database is meaningless — the incremental
-- pull would read nothing. The comparison only means something when both
-- approaches are asked for the same answer after the same day of changes.
-- ===========================================================================

USE collections_demo;
GO

DECLARE @now DATETIME2(3) = SYSDATETIME();

-- ~1% of calls touched today
UPDATE TOP (1) PERCENT collections.dialer
SET update_date = @now;

-- a handful of new calls placed today
INSERT INTO collections.dialer
    (uniqueid, case_id, client_id, telecom_id, phone_number,
     disposition_code, call_date, update_date)
SELECT TOP (5000)
    CONCAT('churn-', NEWID()),
    d.case_id,
    d.client_id,
    d.telecom_id,
    d.phone_number,
    201 + (ABS(CHECKSUM(NEWID())) % 3),
    @now,
    @now
FROM collections.dialer d;

-- ~2% of cases repriced
UPDATE TOP (2) PERCENT collections.[case]
SET actual_capital = actual_capital * 0.97,
    update_date    = @now;

PRINT 'Churn applied.';
GO
