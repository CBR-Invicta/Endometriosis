select
    pv.wizyta_id,
    (
        pv.p1_visit_date :: date - pv.data_urodzenia :: date
    ) / 30 as patient_age
from
    platforma.p1_visits_m pv