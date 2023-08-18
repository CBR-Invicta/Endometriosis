select
    p.ankieta_id,
    p.data_odpowiedzi,
    p.pytanie_id,
    p.id_question,
    p.section_question,
    p.pytanie_eng,
    p.pytanie_pl,
    p.odpowiedzi,
    p.typ_pytania
from
    platforma.platforma_p1_survey p
where
   left(p.stamp, 4) = 'WO_M'
