select
    probe_id,
    label,
    _dlt_load_id as dlt_load_id
from {{ source('lake_probe', 'probe_rows') }}
