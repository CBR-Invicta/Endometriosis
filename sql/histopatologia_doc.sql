select ep.ref_id as patient_id, ed.id as doc_id, short_description, dt.name as document_type_name
from doc.ext_doc ed 
left join doc.ext_type_doc_type etdt on ed.doc_type_id = etdt.ext_type_id 
left join doc.d_type dt on etdt.doc_type_id = dt.id 
join doc.ext_person ep on ed.id = ep.ext_id 
where  short_description ilike ('%%histop%%') or short_description ilike ('%%histp%%') or file_name ilike ('%%histop%%')or file_name ilike ('%%histp%%')