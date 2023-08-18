with wizyty_pierwszorazowe as (
    select
        w."pacjentId" as patient_id,
        wa.wizyta_id,
        w."planowanyCzasOd" as result_time,
        wa.aktywnosc_id,
        w."planowanyLekarzId",
        p.nazwisko
    from
        public.wizyta_aktywnosc wa
        join public.wizyta w on wa.wizyta_id = w.id
        join public.person p on w."planowanyLekarzId" = p.id
    where
        wa.aktywnosc_id in (
            360305,
            1959,
            1942,
            2054 --1427,
            --1426,
            --1561,
            --2097,
            --2098
        )
        and w."statusId" = 7
        and w."planowanyCzasOd" :: date > '2019-01-01' :: date
        and w."planowanyCzasOd" :: date < '2022-04-01' :: date
)
select
    wp.*,
    wo.tresc
from
    wizyty_pierwszorazowe wp
    join public.wizyta_opis wo on wp.wizyta_id = wo.wizyta_id
where
    wo.formularz_id = 61000 -- KLN Badanie fizykalne