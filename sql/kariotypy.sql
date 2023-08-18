select
    z.patient_id_wizyta,
    z.id_wizyta,
    z.original_result,
    z.result_time,
    z.partner_zlecenie
from
    platforma.f_visit_p1_lekarze_zlecenia z
where
    z.analiza_id in (1031 --, 498
)