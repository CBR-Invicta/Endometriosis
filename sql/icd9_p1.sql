with wizyty_pierwszorazowe as (
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
    wp.*,
    wi.procedura_medyczna_id,
    dpm.opis
from
    wizyty_pierwszorazowe wp
    join public.wizyta_icd9 wi on wp.wizyta_id = wi.wizyta_id
    join public.d_procedura_medyczna dpm on wi.procedura_medyczna_id = dpm.id