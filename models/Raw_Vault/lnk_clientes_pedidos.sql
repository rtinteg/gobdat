{{ config(materialized="incremental", unique_key="lnk_cliente_pedido_id") }}

with
    hub_clientes as (
        select hub_cliente_id, nombre_cliente, from {{ source("raw", "HUB_CLIENTES") }}
    ),
    hub_pedidos as (
        select hub_pedido_id, clave_pedido, empleado, fecha_carga
        from {{ source("raw", "HUB_PEDIDOS") }}
    ),
    stg_clientes_pedidos as (
        select a.c_name, b.o_orderkey, b.o_clerk, a.c_origen, b.o_origen, b.load_date
        from {{ source("stg", "STG_CLIENTES") }} a
        join {{ source("stg", "STG_PEDIDOS") }} b
        where a.c_custkey = b.o_custkey
    ),
    combinaciones as (
        select
            md5(
                upper(trim(nvl(p1.nombre_cliente, '')))
                || upper(trim(nvl(clave_pedido, '')))
                || upper(trim(nvl(empleado, '')))
            ) as lnk_cliente_pedido_id,
            p1.hub_cliente_id,
            p2.hub_pedido_id,
            p1.nombre_cliente,
            p2.clave_pedido,
            p2.empleado,
            p3.load_date as fecha_carga,
            p3.c_origen as origen_cliente,
            p3.o_origen as origen_pedido
        from hub_clientes p1, hub_pedidos p2, stg_clientes_pedidos p3
        where
            p1.nombre_cliente = p3.c_name
            and (p2.clave_pedido = p3.o_orderkey and p2.empleado = p3.o_clerk)
    )

select *
from combinaciones

{% if is_incremental() %}
    where lnk_cliente_pedido_id not in (select lnk_cliente_pedido_id from {{ this }})
{% endif %}
