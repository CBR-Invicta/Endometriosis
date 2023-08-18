select
    dps."name" as kategoria,
    a."shortName" nazwa_krotka,
    a."fullName" nazwa_dluga,
    a.external_shortname as nazwa_zewnetrzna,
    dmu."name" as measure_unit,
    dlt."fullName" as laboratory_name,
    dm."name" as material
from
    diag.analysis a
    left join diag."dMeasureUnit" dmu on a."measureUnitId" = dmu.id
    left join diag."dLaboratoryTest" dlt on a."laboratoryTestId" = dlt.id
    left join diag."dMaterial" dm on a."materialId" = dm.id
    left join diag."profileAnalysis" pa on a.id = pa."analysisId"
    left join diag."dProfile" dp on pa."profileId" = dp.id
    left join diag.d_profile_section dps on dp."profileSectionId" = dps.id