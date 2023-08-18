select
    z.patient_id_wizyta,
    z.id_wizyta,
    (z.result :: float / 1000) :: float as result,
    z.partner_zlecenie,
    z.result_time
from
    platforma.f_visit_p1_lekarze_zlecenia z
where
    z.analiza_id in (651, 2531)
    and z.result < 888888