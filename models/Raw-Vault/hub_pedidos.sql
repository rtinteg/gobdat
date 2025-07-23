with
    hub_pedidos as (
        select
            md5(upper(trim(nvl(o_orderkey, '')))) as hub_pedido_id,
            o_orderkey as clave_pedido,
            current_date as fecha_carga,
            o_origen as origen
        from {{ source("stg", "STG_PEDIDOS") }}
    )
select *
from hub_pedidos
