select
    z.patient_id_wizyta,
    z.id_wizyta,
    z.visit_date,
    z.analiza_id,
    (z.result :: float / 1000) :: float as result,
    z.partner_zlecenie,
    z.result_time,
    z.analiza_nazwa
from
    platforma.f_visit_p1_lekarze_zlecenia z
where
    z.analiza_id in (713, 832, 1970, 83, 583, 584, 864, 914, 6)
    and z.partner_zlecenie = false
    and z.result not in (888888000,1000000000)