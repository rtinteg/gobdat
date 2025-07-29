with
    hub_pedidos as (
        select
            md5(
                upper(trim(nvl(o_orderkey, ''))) || upper(trim(nvl(o_clerk, '')))
            ) as hub_pedido_id,
            o_orderkey as clave_pedido,
            o_clerk as empleado,
            current_date as fecha_carga,
            o_origen as origen
        from {{ source("stg", "STG_PEDIDOS") }}
    )
select *
from hub_pedidos
