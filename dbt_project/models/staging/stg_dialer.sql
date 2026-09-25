-- models/staging/stg_dialer.sql
-- ===========================================================================
-- Deduplicated dialer calls, restricted to the clients in scope, with the
-- disposition flags both downstream marts need.
-- ===========================================================================

with source as (
    select * from {{ source('bronze', 'dialer') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by uniqueid
            order by update_date desc, ingest_ts desc
        ) as rn
    from source
)

select
    uniqueid,
    case_id,
    client_id,
    telecom_id,
    phone_number,
    disposition_code,
    call_date,
    cast(call_date as date) as call_date_only,

    case when disposition_code = {{ var('disposition_answered') }}  then 1 else 0 end as is_answered,
    case when disposition_code = {{ var('disposition_failed') }}    then 1 else 0 end as is_failed,
    case when disposition_code = {{ var('disposition_no_answer') }} then 1 else 0 end as is_no_answer

from deduped
where rn = 1
  and client_id in ({{ var('client_ids') | join(', ') }})
