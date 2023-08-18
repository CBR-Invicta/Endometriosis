select
    p.wizyta_id,
    p.response_date as result_time,
    p.id_question,
    p.pytanie_pl,
    p.odpowiedzi
from
    platforma.vm_p1_survey_wizyty p
where
    left(p.stamp, 4) = 'WO_F'
    and p.id_question in (90, 92)