-- models/staging/stg_contacts.sql
-- ===========================================================================
-- Deduplicated contact events, excluding system-generated contacts —
-- matching the source query's contact_stats CTE filter.
--
-- client_id scoping is NOT applied here because the contact table has no
-- client_id column; it is enforced downstream in the mart via the join to
-- stg_cases, which already carries the client filter.
-- ===========================================================================

with source as (
    select * from {{ source('bronze', 'contacts') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by contact_id
            order by update_date desc, ingest_ts desc
        ) as rn
    from source
)

select
    contact_id,
    case_id,
    status_type_id,
    target_type_id,
    contact_date,
    cast(contact_date as date) as contact_date_only,
    insert_user,
    update_date

from deduped
where rn = 1
  and insert_user <> '{{ var("system_contact_user") }}'
