with
    hub_pedidos as (
        select
            md5(upper(trim(nvl(O_ORDERKEY, '')))) as hub_pedido_id,
            O_ORDERKEY as clave_pedido,
            current_date as fecha_carga,
            'Snowflake' as origen
        
        from {{ source("stg","STG_PEDIDOS") }}
    )
select *
from hub_pedidos