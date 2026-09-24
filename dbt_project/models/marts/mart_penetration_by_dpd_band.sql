-- models/marts/mart_penetration_by_dpd_band.sql
-- ===========================================================================
-- Dialer penetration by client x provider x DPD band over a rolling
-- window, with Paid / PTP / right-party contact counts per segment.
--
-- This is the deliverable. Everything upstream — watermark ingestion,
-- staging deduplication — exists to produce this table without re-scanning
-- the source on every run.
-- ===========================================================================

with date_bounds as (
    select
        date_trunc('month', add_months(current_date(), -{{ var('window_months_back') }})) as inicio_mes,
        date_trunc('month', add_months(current_date(), 1))                                as fim_mes
),

active_cases as (

    select
        c.case_id,
        c.client_id,
        coalesce(ca.provider, 'Sem Provider') as provider,
        try_cast(ca.dpd as int)               as dpd_days

    from {{ ref('stg_cases') }} c
    left join {{ ref('stg_case_attributes') }} ca
        on ca.case_id = c.case_id
    -- stg_cases already filters to active cases and the clients in scope

),

active_cases_banded as (

    select
        *,
        case
            when dpd_days between 0   and 30  then 'A: 0-30'
            when dpd_days between 31  and 60  then 'B: 31-60'
            when dpd_days between 61  and 90  then 'C: 61-90'
            when dpd_days between 91  and 180 then 'D: 91-180'
            when dpd_days between 181 and 360 then 'E: 181-360'
            when dpd_days > 360               then 'F: 361+'
            else 'Sem DPD'
        end as dpd_faixa

    from active_cases

),

dialed_cases as (

    select
        d.case_id,
        count(*) as total_calls

    from {{ ref('stg_dialer') }} d
    cross join date_bounds b
    where d.call_date_only >= b.inicio_mes
      and d.call_date_only <  b.fim_mes
    group by d.case_id

),

-- Paid / PTP / right-party counts per case, scoped to active cases in the
-- clients in scope, within the same rolling window as dialed_cases.
-- stg_contacts already excludes system-generated contacts.
contact_stats as (

    select
        ct.case_id,

        count(distinct case when ct.status_type_id = {{ var('status_paid') }}
                            then ct.contact_id end) as paid_count,

        count(distinct case when ct.status_type_id in ({{ var('status_ptp') | join(', ') }})
                            then ct.contact_id end) as ptp_count,

        count(distinct case when ct.target_type_id in ({{ var('target_right_party') | join(', ') }})
                            then ct.contact_id end) as cc_count

    from {{ ref('stg_contacts') }} ct
    inner join active_cases_banded ac
        on ac.case_id = ct.case_id      -- enforces active + client scope
    cross join date_bounds b

    where ct.contact_date_only >= b.inicio_mes
      and ct.contact_date_only <  b.fim_mes

    group by ct.case_id

)

select
    ac.client_id,
    ac.provider,
    ac.dpd_faixa,

    count(ac.case_id)                                   as quantidade,
    count(dc.case_id)                                   as acionado,
    count(ac.case_id) - count(dc.case_id)               as sem_ac,

    round(count(dc.case_id) * 1.0 / nullif(count(ac.case_id), 0), 4) as penetration_rate,

    sum(coalesce(dc.total_calls, 0))                    as total_calls,

    sum(coalesce(cs.paid_count, 0))                     as paid,
    sum(coalesce(cs.ptp_count, 0))                      as ptp,
    sum(coalesce(cs.cc_count, 0))                       as cc

from active_cases_banded ac
left join dialed_cases  dc on dc.case_id = ac.case_id
left join contact_stats cs on cs.case_id = ac.case_id

group by
    ac.client_id,
    ac.provider,
    ac.dpd_faixa

-- No ORDER BY: this materialises as a table and ordering is not preserved
-- on read. It only bought a shuffle at build time. Power BI sorts anyway.
