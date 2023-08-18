select
    z.id_wizyta,
    z.result_time,
    z.original_result,
    z.analiza_id
from
    platforma.f_visit_p1_lekarze_zlecenia z
where
    z.analiza_id in (651, 2531, 841, 2532, 2371)