select
    p.wizyta_id,
    p.patient_id,
    p.response_date,
    p.ankieta_uuid,
    p.id_form,
    p.id_question,
    p.section_question,
    p.pytanie_eng,
    p.pytanie_pl,
    p.odpowiedzi
from
    platforma.vm_p1_survey_wizyty_m p
where
    left(p.stamp, 4) = 'WO_M'