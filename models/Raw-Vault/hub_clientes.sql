select 
    md5(upper(trim(nvl(c_name,'')))) as HUB_cliente_id,
    c_name as NOMBRE_CLIENTE,
    current_date as FECHA_CARGA,
    'Snowflake' as ORIGEN
from {{ source("stg", "stg_clientes") }}
-- from {{ ref("stg_clientes")}}
