/*
--verified
Zwraca tabele zawierajaca dane dot. wybranych wizyt pacjentki z informacja o wizycie poprzedzajacej oraz nastepujacej.

*/


select
    wa.wizyta_id,
    wa.aktywnosc_id,
    g.nazwa,
    a.nazwa as aktywnosc_nazwa,
    w."pacjentId" as patient_id,
    case 
		when lag (w."planowanyCzasOd", 1) over (partition by w."pacjentId" order by  w."planowanyCzasOd") is null then  '0001-01-01'::date 
		else lag (w."planowanyCzasOd", 1) over (partition by w."pacjentId" order by w."planowanyCzasOd")
	end previous_visit_date,    
    w."planowanyCzasOd" as data_wizyty,
    case 
		when lead (w."planowanyCzasOd", 1) over (partition by w."pacjentId" order by w."planowanyCzasOd") is null then '9999-12-31'::date
		else lead (w."planowanyCzasOd", 1) over (partition by w."pacjentId" order by w."planowanyCzasOd")
	end next_visit_date,
	case 
		when w."planowanyCzasOd"::date = lead (w."planowanyCzasOd"::date, 1) over (partition by w."pacjentId" order by w."planowanyCzasOd"::date) then true 
		else false end following_same_date_p1_visit,
	w.dorejestrowana 
from public.wizyta_aktywnosc wa
join public.wizyta w on w.id = wa.wizyta_id 
join public.gabinet g on g.id = w.gabinet_id --and g.nazwa not ilike '%' --and g.nazwa = 'KLN'
join public.person p on p.id = w."pacjentId" and p.plec_id = 2
join public.aktywnosc a on a.id = wa.aktywnosc_id 
where
	w."planowanyCzasOd" ::date between '2019-01-01'::date and current_date
	and w."statusId"=any(array[4,7])
	and wa.aktywnosc_id in ( 360305,
                1959,
                1942,
                2054,
                1427,
                1426,
                1561,
                2097,
                2098,
                360334,
                360313,
                180318,
                181125,
                281,
                983) 
	--and w."pacjentId" = 13802

order by patient_id, data_wizyty; 
