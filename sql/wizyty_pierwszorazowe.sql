select
    wa.wizyta_id,
    wa.aktywnosc_id
from
    public.wizyta_aktywnosc wa
    join public.wizyta w on wa.wizyta_id = w.id
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
        2098
    )
    and w."statusId" = 7
    and w."planowanyCzasOd" :: date > '2019-01-01' :: date
    and w."planowanyCzasOd" :: date < '2022-04-01' :: date -- Porada pierwszorazowa (id: 360305)
    -- ZOOM porada pierwszorazowa (id: 1959)
    -- Porada pierwszorazowa Program Miejski (id: 1942)
    -- ZOOM- porada pierwszorazowa Program Miejski (id: 2054)
    -- Konsultacja po PROGRAMIE IVF PROMOCJA (id: 1427)
    -- Konsultacja po PROGRAMIE IVF (id: 1426)
    -- Konsultacja po zabiegu inseminacji – PROMO ( id: 1561)
    -- Porada w KLN - po przerwie (id: 2097)
    -- ZOOM - porada w KLN po przerwie (id: 2098)