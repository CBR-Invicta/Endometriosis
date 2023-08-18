select 
    w."pacjentId" as patient_id,
    wo.wizyta_id, wo.tresc
    
from 
    public.wizyta_opis wo
    join public.wizyta w on wo.wizyta_id = w.id