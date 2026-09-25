-- models/staging/stg_case_attributes.sql
-- ===========================================================================
-- Pivots case_attribute (long format) into one row per case_id with
-- provider and dpd as columns.
--
-- Deduplication happens first (latest known value per case_id x type_id),
-- THEN the pivot — otherwise a case with multiple historical updates to the
-- same attribute type would double-count in the max(case when) pivot.
-- ===========================================================================

with source as (
    select * from {{ source('bronze', 'case_attributes') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by case_id, case_attribute_type_id
            order by update_date desc, ingest_ts desc
        ) as rn
    from source
),

deduped_filtered as (
    select * from deduped where rn = 1
)

select
    case_id,
    max(case when case_attribute_type_id = {{ var('attr_type_provider') }}
             then case_attribute_value end) as provider,
    max(case when case_attribute_type_id = {{ var('attr_type_dpd') }}
             then case_attribute_value end) as dpd

from deduped_filtered
group by case_id
