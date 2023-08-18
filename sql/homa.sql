select
    z.id_wizyta,
    (z.result :: float / 1000) :: float as result,
    z.partner_zlecenie,
    z.result_time,
    z.analiza_id
from
    platforma.f_visit_p1_lekarze_zlecenia z
where
    z.analiza_id in (841, 2532, 2371, 651, 2531)
    and z.result < 888888
    and z.partner_zlecenie is FALSE