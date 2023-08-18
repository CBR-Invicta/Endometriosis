with wizyty as (
    select
        wa.wizyta_id,
        wa.aktywnosc_id,
        w."planowanyLekarzId",
        p.nazwisko
    from
        public.wizyta_aktywnosc wa
        join public.wizyta w on wa.wizyta_id = w.id
        join public.person p on w."planowanyLekarzId" = p.id
    where
        w."statusId" = 7
        and w."planowanyCzasOd" :: date > '2018-01-01' :: date
        and w."planowanyCzasOd" :: date < '2022-04-01' :: date
)
select
    wp.*,
    wo.formularz_id,
    wo.tresc
from
    wizyty wp
    join public.wizyta_opis wo on wp.wizyta_id = wo.wizyta_id
where
    wo.formularz_id in (61000) -- KLN Badanie fizykalne