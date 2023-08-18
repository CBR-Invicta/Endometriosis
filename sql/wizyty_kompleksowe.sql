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
        and w."planowanyCzasOd" :: date > '2021-01-01' :: date
)
select
    a.nazwa as nazwa_aktywnosci,
    wa.aktywnosc_id,
    v.*,
    j.nazwa as nazwa_jednostki,
    g.nazwa as nazwa_gabinetu,
    g.lokalizacja_id,
    l.nazwa as nazwa_lokalizacji,
    tw.nazwa as nazwa_wizyty,
    wk.provision_desc,
    wk.requires_treatment,
    wk.requires_treatment_desc,
    wk.not_requires_treatment,
    wk.not_requires_treatment_desc,
    wk.referral_hospital,
    wk.hints_for_referring_doctor,
    wk.other_specialization,
    wk.end_specialistic_treatment,
    wk.poz_treatment
from
    visits v
    join public.wizyta_kompleksowa wk on wk.wizyta_id = v.id_wizyty
    left join public.wizyta_aktywnosc wa on wa.wizyta_id = v.id_wizyty
    join public.aktywnosc a on a.id = wa.aktywnosc_id
    left join d_jednostka j on v."jednOrgId" = j.id
    left join gabinet g on v.gabinet_id = g.id
    left join lokalizacja l on g.lokalizacja_id = l.id
    left join typ_wizyty tw on v.typ_wizyty_id = tw.id
where
    wk.id is not null
    and g.lokalizacja_id != 20 -- KAA Madison
    and l.id not in (213, 214, 215, 191, 193, 210, 192) -- 'KAA Gabinet Kosmetologiczny','KAA Gabinet Podologiczny','KAA Medycyna Estetyczna','KOS 1','KOS 2','Laguna','MES'