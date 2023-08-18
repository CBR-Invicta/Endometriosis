/*
dotyczy badań wykonanych przez daną pacjentkę między kolejnymi wizytami P1, lub jeśli była tylko jedna P1 wszystkcih badań po tej wizycie
*/

select t.* from (
	with p1_visit as (
		select 
			wa.wizyta_id,
			w."pacjentId" as patient_id,
			wa.aktywnosc_id,
			g.nazwa,
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
			w."planowanyCzasOd" ::date between '2019-01-01'::date and current_date
			and w."statusId"=any(array[4,7])
			and wa.aktywnosc_id in (360305, 1959, 1942, 2054, 1427, 1426, 1561, 2097, 2098) 
	-- and w."pacjentId" = 13802
		order by patient_id, p1_visit_date 
		)
		
	select 
		v.*, 
		o.id as order_id, 
		o.number AS order_number,
		o."collectionTime" AS collection_time,
		o."registrationTime" , 
		extract(year from age( o."collectionTime"::date, v.data_urodzenia)) as wiek_przy_pobraniu,
		o."cycleDay" AS cycle_day,
		o."cyclePhase"::text AS cycle_phase,
		o.stimulation_day,
		o."isSmoked" AS is_smoker,
		o.pregnant,
		vpa."resultTime",--vpa."profileAnalysisId" , 
		dp.dynamic AS profile_dynamic,
		dp."name" as profil_analiza, 
		dp."shortName" as profil_analiza_short ,
	--vpa."apparatusId",
		rv.apparatus_id,
		a.id as analiza_id,
		a."fullName" as analiza_nazwa,
		vpa."originalResult" as original_result,
		rv."originalValueFrom" as original_norma_dol, 
		rv."originalValueTo" as original_norma_gora,
		vpa."result",
		rv."valueFrom" as norma_dol, 
		rv."valueTo" as norma_gora, 
		dmu."name" as wynik_jednostka,
		case 
			when vpa."result" not between rv."valueFrom" and rv."valueTo" then false 
			else true 
		end result_in_range,
		rv."ageFrom" as norma_wiek_dol, 
		rv."ageTo" as norma_wiek_gora,
		rv.sex,rv."validFrom",rv."validTo" ,
		case 
		    when lead (rv."validFrom", 1) over (partition by v.wizyta_id, v.patient_id, o."collectionTime", vpa."resultTime", a.id order by rv."validFrom") != rv."validFrom"
		    then lead (rv."validFrom", 1) over (partition by v.wizyta_id, v.patient_id, o."collectionTime", vpa."resultTime", a.id order by rv."validFrom")
		end vir_valid_to,
		diag.analysis_ref_value(a.id::int) as opis_zakresu,
		rv.d_standard_sources as regula_kontrolna,
		dss.opis as regula_kontrolna_opis
	from p1_visit v
		left join diag."order" o on o."patientId" = v.patient_id and v.p1_visit_date <= o."collectionTime" 
		left join diag."validatedProfileAnalysis" vpa on vpa."orderId" = o.id
		join diag."profileAnalysis" pa on pa.id = vpa."profileAnalysisId" 
		join diag.analysis a on a.id = pa."analysisId" 
		join diag."dProfile" dp on dp.id = pa."profileId" 
		join diag."dMeasureUnit" dmu on dmu.id = a."measureUnitId" 
		left join diag."referenceValue" rv on rv."analysisId" = a.id 
			and rv.apparatus_id = vpa."apparatusId" 
			and age( o."collectionTime"::date, v.data_urodzenia::date) between rv."ageFrom" and rv."ageTo" 
		left join diag.d_standard_sources dss on dss.id = rv.d_standard_sources 
	where o."collectionTime"  between v.p1_visit_date and v.next_p1_visit_date 
	--and a.id = 779-- 83-- 779
	and vpa."resultTime"::date between rv."validFrom"::date and rv."validTo"::date 
	and rv.sex = v.plec_id ) t 
where t.vir_valid_to is null


