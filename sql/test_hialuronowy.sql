select
    z.id_wizyta,
    z.result / 1000 as result,
    z.partner_zlecenie,
    z.result_time
from
    platforma.f_visit_p1_lekarze_zlecenia z
where
    z.analiza_id in (1803 --, 498
)