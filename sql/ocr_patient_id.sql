select ep.ref_id as patient_id, ed.id, ed.doc_type_id , ed.file_name 
from doc.ext_doc ed 
left join doc.ext_type_doc_type etdt on ed.doc_type_id = etdt.ext_type_id 
left join doc.d_type dt on etdt.doc_type_id = dt.id
left join doc.ext_person ep on ed.id = ep.ext_id
where dt."name" in ('Analityka ogólna','Badanie genetyczne','Badanie hormonalne','Badanie infekcyjne','Hematologia','Koagulogia')