select distinct(o."patientId" )
from diag."order" o 
    join diag."validatedProfileAnalysis" vpa on vpa."orderId" = o.id
    join diag."profileAnalysis" pa on pa.id = vpa."profileAnalysisId"
    join diag.analysis anal on anal.id = pa."analysisId"
    join diag."dProfile" dp on dp.id = pa."profileId"
    where dp."laboratoryId" in (6,15)