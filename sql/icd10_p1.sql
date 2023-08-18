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
    wi.rozpoznanie_medyczne_id,
    drm.opis
from
    wizyty_pierwszorazowe wp
    join public.wizyta_icd10 wi on wp.wizyta_id = wi.wizyta_id
    join public.d_rozpoznanie_medyczne drm on wi.rozpoznanie_medyczne_id = drm.id