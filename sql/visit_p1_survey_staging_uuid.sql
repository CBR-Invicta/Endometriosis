select
    distinct p.ankieta_id,
    p.uuid_ankieta :: TEXT
from
    platforma.vm_p1_surveys p