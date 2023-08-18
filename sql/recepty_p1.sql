with wizyty_p1 as(
    select
        wa.wizyta_id,
        wa.aktywnosc_id
    from
        public.wizyta_aktywnosc wa
        join public.wizyta w on wa.wizyta_id = w.id
    where
        wa.aktywnosc_id in (
            360305,
            1959,
            1942,
            2054,
            1427,
            1426,
            1561,
            2097,
            2098
        )
        and w."statusId" = 7
        and w."planowanyCzasOd" :: date > '2019-01-01' :: date
        and w."planowanyCzasOd" :: date < '2022-04-01' :: date
)
select
    pp.patient_id,
    pp.meeting_id,
    pp.prescriptions,
    pp."when"
from
    wizyty_p1
    join public.prescription_package pp on wizyty_p1.wizyta_id = pp.meeting_id