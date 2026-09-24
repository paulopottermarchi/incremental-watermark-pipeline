-- models/marts/mart_phone_quality.sql
-- ===========================================================================
-- Phone-level call quality signals used to flag telecom_ids for dialer
-- campaign cleanup.
--
-- Bounded to the same rolling window as the penetration mart. Without it
-- this model scanned the entire Bronze history on every run — and since
-- Bronze is append-only, that cost grows forever while the cleanup decision
-- itself only ever concerns recent call behaviour. A number failing today
-- is the signal; a number that failed eighteen months ago is not.
-- ===========================================================================

with date_bounds as (
    select
        date_trunc('month', add_months(current_date(), -{{ var('window_months_back') }})) as inicio_mes,
        date_trunc('month', add_months(current_date(), 1))                                as fim_mes
),

dialer as (

    select d.*
    from {{ ref('stg_dialer') }} d
    cross join date_bounds b
    where d.call_date_only >= b.inicio_mes
      and d.call_date_only <  b.fim_mes

),

telecom_metrics as (

    select
        client_id,
        case_id,
        telecom_id,
        max(phone_number) as phone_number,
        count(*)          as total_calls,
        sum(is_answered)  as answered_calls,
        sum(is_failed)    as failed_calls,
        sum(is_no_answer) as no_answer_calls

    from dialer
    group by client_id, case_id, telecom_id

)

select
    client_id,
    case_id,
    telecom_id,
    phone_number,
    total_calls,
    answered_calls,
    failed_calls,
    no_answer_calls,

    round(failed_calls   * 1.0 / nullif(total_calls, 0), 4) as pct_failed,
    round(answered_calls * 1.0 / nullif(total_calls, 0), 4) as pct_answered,

    case
        when total_calls  > {{ var('cleanup_min_total_calls') }}
         and failed_calls > {{ var('cleanup_min_failed_calls') }}
        then 1
        else 0
    end as is_cleanup_candidate

from telecom_metrics
