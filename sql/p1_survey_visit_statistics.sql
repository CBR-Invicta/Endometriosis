select
       --wa.wizyta_id,
     mar.id_meeting as wizyta_id,
    w."pacjentId" as patient_id,
    s.uuid as p1_survey_uuid,
    s.insert_time as data_ankiety,
    a.nazwa as aktywnosc_nazwa,
mar.id_activity as aktywnosc_id,
    --wa.aktywnosc_id,
    g.nazwa as miejsce_wizyty,
    case 
        when lag (w."planowanyCzasOd", 1) over (partition by w."pacjentId" order by  w."planowanyCzasOd") is null then  '0001-01-01'::date 
        else lag (w."planowanyCzasOd", 1) over (partition by w."pacjentId" order by w."planowanyCzasOd")
    end previous_p1_visit_date,
    w."planowanyCzasOd" as p1_visit_date,
    case 
        when lead (w."planowanyCzasOd", 1) over (partition by w."pacjentId" order by w."planowanyCzasOd") is null then '9999-12-31'::date
        else lead (w."planowanyCzasOd", 1) over (partition by w."pacjentId" order by w."planowanyCzasOd")
    end next_p1_visit_date,
    case 
        when w."planowanyCzasOd"::date = lead (w."planowanyCzasOd"::date, 1) over (partition by w."pacjentId" order by w."planowanyCzasOd"::date) then true 
    else false end following_same_date_p1_visit,
    w.dorejestrowana,
    p.plec_id,
    p.data_urodzenia
--from public.wizyta_aktywnosc wa
from public.meeting_activity_real mar
    --join public.wizyta w on w.id = wa.wizyta_id 
    join public.wizyta w on w.id = mar.id_meeting 
    join public.gabinet g on g.id = w.gabinet_id
    join public.person p on p.id = w."pacjentId" --and p.plec_id = 2
    --join public.aktywnosc a on a.id = wa.aktywnosc_id 
    join public.aktywnosc a on a.id = mar.id_activity
    left join survey.wo_survey s on s.receiver_id = w."pacjentId" and s.insert_time <= w."planowanyCzasOd" -- wa.wizyta_id = any(s.visits_id)--    s.receiver_id = w."pacjentId" 
where
    w."planowanyCzasOd" ::date between '2005-01-01'::date and current_date
    and w."statusId"=any(array[4,7])
      --and wa.aktywnosc_id in (360305, 1959, 1942, 2054, 1427,  1561, 2097, 2098)  --1426,
    and mar.id_activity in (360305, 1959, 1942, 2054, 1427,  1561, 2097, 2098)  ---1426 usuniete
order by patient_id, p1_visit_date; 