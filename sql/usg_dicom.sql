select
    vief.visit_time as result_time,
    vief.form_desc as tresc,
    vief.process_nr,
    vief.patient_id,
    vief.visit_id as wizyta_id
from
    dicom_comm.v_imaging_examination_forms vief
    join public.wizyta_aktywnosc wa on vief.visit_id = wa.wizyta_id
where
    vief.visit_status = 7
    and vief.visit_time :: date > '2019-01-01' :: date
    and vief.visit_time :: date < '2022-04-01' :: date
    and wa.aktywnosc_id in (
        360305,
        1959,
        1942,
        2054 --1427,
        --1426,
        --1561,
        --2097,
        --2098
    )