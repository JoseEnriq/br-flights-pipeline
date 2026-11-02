-- Fails when fewer than 95% of the flight routes ANAC and the flight model
-- share match in any month. The gap that remains comes from ANAC itself: its
-- percentages count more cancellations than its own flight files show, by
-- airline and flight number, worst in March 2026 (96.0% matching; 97.7% to
-- 99.7% in the other months up to August 2026). A change in the duplicate
-- rule or in the delay definition would fall far below the threshold.
select
    "month",
    flight_routes_in_both,
    flight_routes_matching,
    match_rate
from {{ ref('audit_anac_reconciliation') }}
where match_rate < 0.95
