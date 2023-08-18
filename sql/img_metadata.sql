select
    ed.id,
    dt."name",
    ed.import_time,
    ed.short_description,
    ed.import_user,
    p.imie,
    p.nazwisko,
    p.data_urodzenia
from
    doc.ext_doc ed
    left join doc.ext_type_doc_type etdt on ed.doc_type_id = etdt.ext_type_id
    left join doc.d_type dt on etdt.doc_type_id = dt.id
    left join doc.ext_person ep on ed.id = ep.ext_id
    left join public.person p on ep.ref_id = p.id
where
    dt."name" in (
        'Analityka ogólna',
        'Badanie genetyczne',
        'Badanie hormonalne',
        'Badanie infekcyjne',
        'Hematologia',
        'Koagulogia'
    )