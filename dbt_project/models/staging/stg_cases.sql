-- models/staging/stg_cases.sql
-- ===========================================================================
-- Deduplicated latest state per case, restricted to active cases for the
-- clients in scope — same filter as the source query's active_cases CTE.
--
-- The deduplication is not cosmetic. Bronze is an append-only change log,
-- and the ingestion layer deliberately re-reads a small overlap window on
-- every run, so the same case_id legitimately appears more than once.
-- ===========================================================================

with source as (
    select * from {{ source('bronze', 'cases') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by case_id
            order by update_date desc, ingest_ts desc
        ) as rn
    from source
)

select
    case_id,
    client_id,
    ref_number,
    debtor_id,
    original_capital,
    actual_capital,
    case_statute_id,
    update_date

from deduped
where rn = 1
  and case_statute_id = {{ var('active_case_statute_id') }}
  and client_id in ({{ var('client_ids') | join(', ') }})
