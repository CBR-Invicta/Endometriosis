select
    m.*
from
    public."makroTekstowe" m
where
    m."poleProgramu" in (
        'Badanie fizykalne',
        'Badanie fizykalne KOS/MES',
        'Badanie fizykalne txt',
        'KLN Badanie fizykalne',
        'KLN Badanie fizykalne txt'
    )
    and m."skrot" = 'U';