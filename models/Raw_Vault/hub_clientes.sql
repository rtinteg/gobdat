with
    hub_clientes as (
        select
            md5(upper(trim(nvl(c_name, '')))) as hub_cliente_id,
            c_name as nombre_cliente,
            current_date as fecha_carga,
            c_origen as origen
        from {{ source("stg", "STG_CLIENTES") }}
    )
select *
from hub_clientes
