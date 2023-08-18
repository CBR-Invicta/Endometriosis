/*
--verified
Zwraca tabele gdzie do zlecenia diagnostycznego lekarza przypisano wnyik badania
pacjentki oraz pacjenta - jeśli badanie zostało wykonane.
Tabela zawiera rowniez zakresy wynikow, i okresla czy wynik byl w normie

*/


select
    x.*
from
    (
        with t_references_value as (
            select
                distinct on (
                    rvv."analysisId",
                    rvv.sex,
                    rvv."ageFrom",
                    rvv."ageTo",
                    rvv."valueFrom",
                    rvv.apparatus_id,
                    rvv.method_local_id,
                    rvv.d_standard_sources
                ) 
                rvv."analysisId",
                rvv.apparatus_id,
                rvv."originalValueFrom",
                rvv."originalValueTo",
                rvv."valueFrom",
                rvv."valueTo",
                rvv."ageFrom",
                rvv."ageTo",
                rvv.sex,
                rvv."validFrom",
                rvv."validTo",
                rvv.d_standard_sources


            from
                diag."referenceValue" rvv
        )
        select
            wz.id_wizyta,
            wa.aktywnosc_id,
            akt.nazwa as visit_name,
            --wz.id_order,
            wz."when" as wizyta_zlecenia_czas,
            g.nazwa as gabinet,
            w."pacjentId" as patient_id_wizyta,
            o."patientId" as patient_id_zlecenie,
            case
                when w."pacjentId" != o."patientId" then true
                else false
            end partner_zlecenie,
            w."planowanyCzasOd" as visit_date,
            o.id as order_id,
            o.number AS order_number,
            o."registrationTime",
            o."collectionTime" AS collection_time,
            vpa."resultTime",
            dp.dynamic AS profile_dynamic,
            dp."name" as profil_analiza,
            dp."shortName" as profil_analiza_short,
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
                when vpa."result" not between rv."valueFrom"
                and rv."valueTo" then false
                else true
            end result_in_range,
            rv."ageFrom" as norma_wiek_dol,
            rv."ageTo" as norma_wiek_gora,
            rv.sex,
            rv."validFrom",
            rv."validTo",
            case
                when lead (rv."validFrom", 1) over (partition by w.id, w."pacjentId", o."collectionTime", vpa."resultTime", a.id order by rv."validFrom") != rv."validFrom" 
                then lead (rv."validFrom", 1) over (partition by w.id, w."pacjentId", o."collectionTime", vpa."resultTime", a.id order by rv."validFrom")
            end vir_valid_to,
            diag.analysis_ref_value(a.id :: int) as opis_zakresu,
            rv.d_standard_sources as regula_kontrolna,
            dss.opis as regula_kontrolna_opis
        from
            public.wizyta_zlecenie wz
            join public.wizyta_aktywnosc wa on wz.id_wizyta = wa.wizyta_id
            join public.aktywnosc akt on akt.id = wa.aktywnosc_id
            join public.wizyta w on w.id = wz.id_wizyta
            join diag."order" o on o.id = wz.id_order
            join public.person p on p.id = w."pacjentId"
            left join diag."validatedProfileAnalysis" vpa on vpa."orderId" = o.id
            join diag."profileAnalysis" pa on pa.id = vpa."profileAnalysisId"
            join diag.analysis a on a.id = pa."analysisId"
            join public.gabinet g on g.id = w.gabinet_id
            join diag."dProfile" dp on dp.id = pa."profileId"
            join diag."dMeasureUnit" dmu on dmu.id = a."measureUnitId" --left join diag."referenceValue" rv on rv."analysisId" = a.id 
            left join t_references_value rv on rv."analysisId" = a.id
                and rv.apparatus_id = vpa."apparatusId"
                and rv.sex = p.plec_id
                and age(o."collectionTime" :: date, p.data_urodzenia :: date) between rv."ageFrom" and rv."ageTo"
            left join diag.d_standard_sources dss on dss.id = rv.d_standard_sources
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
                2098,
                360334,
                360313,
                180318,
                181125,
                281,
                983 --porada z wynikami badan
            )
            and w."statusId" = any(array [4,7])
            --and w."pacjentId" = 13802
    order by wz.id_wizyta, w."planowanyCzasOd"::date
    ) x
where
    x.vir_valid_to is null
order by x.id_wizyta, x.visit_date
