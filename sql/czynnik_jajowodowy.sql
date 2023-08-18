select
    w."pacjentId" as patient_id,
    wu.wizyta_id,
    w."planowanyCzasOd" as result_time,
    wu.zwezenie_kanalu_szyjki_macicy,
    wu.zwiekszony_opor_przy_podawaniu_plynu,
    wu.silny_opor_przy_podawaniu_plynu,
    wu.jama_macicy_zwyklego_ksztaltu_i_pojemnosci,
    wu.nieprawidlowe_echo_w_jamie_macicy,
    wu.jajowod_prawy_drozny,
    wu.podejrzenie_niedroznosci_jajowodu_prawego,
    wu.jajowod_lewy_drozny,
    wu.podejrzenie_niedroznosci_jajowodu_lewego,
    wu.plyn_swobodnie_splywa_do_zatoki_douglasa,
    wu.plyn_gromadzi_sie_w_poblizu_prawego_rogu_macicy,
    wu.plyn_gromadzi_sie_w_poblizu_lewego_rogu_macicy
from
    public.wizyta_usghsg wu
    join public.wizyta w on wu.wizyta_id = w.id