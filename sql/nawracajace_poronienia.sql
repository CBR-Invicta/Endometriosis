select
    p.wizyta_id,
    p.response_date as result_time,
    case
        when p.odpowiedzi != '"NO"' then 1
        else 0
    END odpowiedz
from
    platforma.vm_p1_survey_wizyty p
where
    left(p.stamp, 4) = 'WO_F'
    and p.id_question = 569