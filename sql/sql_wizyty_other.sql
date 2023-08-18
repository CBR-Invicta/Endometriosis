with visits as (
    select
        w.id as id_wizyty,
        w."pacjentId",
        w."planowanyCzasOd",
        w."planowanyLekarzId",
        w."jednOrgId",
        w.gabinet_id,
        w."pierwszyRaz",
        w.typ_wizyty_id,
        w.platnik_id,
        w."comment" as komentarz_do_wizyty,
        w.nfz_meeting_type_id
    FROM
        public.wizyta w
    WHERE
        w."statusId" = 7
        and w."planowanyCzasOd" :: date > '2018-01-01' :: date
)
SELECT
    a.nazwa as nazwa_aktywnosci,
    wa.aktywnosc_id,
    v.*,
    j.nazwa as nazwa_jednostki,
    g.nazwa as nazwa_gabinetu,
    g.lokalizacja_id,
    l.nazwa as nazwa_lokalizacji,
    tw.nazwa as nazwa_wizyty,
    f.nazwa AS visit_type,
    wo.tresc AS hints_for_referring_doctor
FROM
    visits v
    inner join public.wizyta_opis wo ON wo.wizyta_id = v.id_wizyty
    left join public.wizyta_aktywnosc wa on wa.wizyta_id = v.id_wizyty
    join public.aktywnosc a on a.id = wa.aktywnosc_id
    left join d_jednostka j on v."jednOrgId" = j.id
    left join gabinet g on v.gabinet_id = g.id
    left join lokalizacja l on g.lokalizacja_id = l.id
    left join typ_wizyty tw on v.typ_wizyty_id = tw.id
    left join d_formularz f ON f.id = wo.formularz_id
where
    g.lokalizacja_id != 20 -- KAA Madison
    and l.id not in (213, 214, 215, 191, 193, 210, 192) -- 'KAA Gabinet Kosmetologiczny','KAA Gabinet Podologiczny','KAA Medycyna Estetyczna','KOS 1','KOS 2','Laguna','MES'
    and f.id in (
        62000,
        61000,
        64000,
        67000,
        100003,
        100004,
        100005,
        100006,
        63000,
        100007,
        66000,
        65000,
        100104
    ) -- wszystkie, gdzie KLN z przodu + Kwalifikacja do programu, Kwalifikacja do Programu Rządowego, PROGRAM IVF ED FRESH 6-3-1, PROGRAM IVF ED FROZEN 6-3-1