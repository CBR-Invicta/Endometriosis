/*
--verified
dotyczy badań wykonanych przez daną pacjentkę tylko przed wizytą P1. Założono, że to "pierwsza" P1 na podstawie id aktywnosci
lub nazwy. Uwzgledniono miejsce wizyty jako KLN. 
*/


	with p1_visit as (
		select 
			wa.wizyta_id,
			w."pacjentId" as patient_id,
			wa.aktywnosc_id,
			g.nazwa as gabinet_nazwa,
			a.nazwa as aktywnosc_nazwa,
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
            p.plec_id, --2 kobieta
            p.data_urodzenia	    
		from public.wizyta_aktywnosc wa
			join public.wizyta w on w.id = wa.wizyta_id 
			join public.gabinet g on g.id = w.gabinet_id
			join public.person p on p.id = w."pacjentId" and p.plec_id = 2
			join public.aktywnosc a on a.id = wa.aktywnosc_id 
		where
			w."planowanyCzasOd" ::date between '2000-01-01'::date and current_date
			and w."statusId"=any(array[4,7])
			and (wa.aktywnosc_id in (360305, 1959, 1942, 2054, 1427, 1426, 1561, 2097, 2098) 
			or a.nazwa  ilike '%pierwszorazowa%')
			and g.nazwa not ilike '%kzk%'
		order by patient_id, p1_visit_date 
		)
		
	select 
		v.wizyta_id, 
		v.patient_id, 
		v.aktywnosc_id, 
		v.gabinet_nazwa, 
		v.aktywnosc_nazwa, 
		v.previous_p1_visit_date, 
		v.p1_visit_date, 
		v.next_p1_visit_date, 
		v.following_same_date_p1_visit, 
		v.dorejestrowana, v.plec_id, v.data_urodzenia,
		o.id as order_id, 
		o.number AS order_number,
		o."collectionTime" AS collection_time,
		o."registrationTime",
        case 
            when  o."registrationTime"::date  >= v.p1_visit_date::date  then 'Diagnostyka Po P1'
            when  o."registrationTime"::date  < v.p1_visit_date::date  then 'Diagnostyka Przed P1'
		else null end status_diagnostyka,
		age( o."collectionTime"::date, v.p1_visit_date::date) as age_od_p1,
		(o."collectionTime"::date-v.p1_visit_date::date) as dni_od_p1,
		extract(year from age( o."collectionTime"::date, v.data_urodzenia)) as wiek_przy_pobraniu,
		vpa."resultTime",
		dp."name" as profil_analiza, 
		dp."shortName" as profil_analiza_short ,
		a.id as analiza_id,
		a."fullName" as analiza_nazwa,
		vpa."originalResult" as original_result,
		vpa."result",
		dmu."name" as wynik_jednostka
	
	from p1_visit v
		left join diag."order" o on o."patientId" = v.patient_id --and v.p1_visit_date <= o."collectionTime" 
		left join diag."validatedProfileAnalysis" vpa on vpa."orderId" = o.id
		join diag."profileAnalysis" pa on pa.id = vpa."profileAnalysisId" 
		join diag.analysis a on a.id = pa."analysisId" 
		join diag."dProfile" dp on dp.id = pa."profileId" 
		join diag."dMeasureUnit" dmu on dmu.id = a."measureUnitId" 
		
	
	where 
        v.p1_visit_date>= '2019-01-01'::date
        and v.previous_p1_visit_date = '0001-01-01'::date
        and ((o."collectionTime"::date-v.p1_visit_date::date)<= 31 and (o."collectionTime"::date-v.p1_visit_date::date) >=-31)
	order by v.patient_id , v.p1_visit_date, o."registrationTime"::date, status_diagnostyka, dni_od_p1;