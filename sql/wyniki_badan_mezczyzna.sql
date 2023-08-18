select
    z.patient_id_wizyta,
    z.patient_id_zlecenie,
    z.id_wizyta as wizyta_id,
    z.analiza_id,
    (z.result :: float / 1000) :: float as result,
    z.partner_zlecenie,
    z.result_time,
    z.analiza_nazwa,
    z.original_result
from
    platforma.f_visit_p1_lekarze_zlecenia z
where
    z.analiza_id in (610, 600, 596, 599, 611, 2310, 1032, 2275, 701, 1803, 2215, 799, 614, 2308, 918, 2310, 1032, 805, 955, 602, 603, 605, 608, 613, 2309)
    and z.partner_zlecenie = True
    and z.result not in (888888000,999999000, 1000000000)
