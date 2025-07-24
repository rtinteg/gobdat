with
    hub_pedidos as (
        select hub_pedido_id, clave_pedido, empleado
        from {{ source("raw", "HUB_PEDIDOS") }}
        where empleado in ('Clerk#000000801', 'Clerk#000000085')
    ),
    hub_lineas_pedidos as (
        select hub_lineas_pedido_id, clave_pedido, linea_pedido
        from {{ source("raw", "HUB_LINEAS_PEDIDOS") }}
        where clave_pedido in (4153445, 4801794)
    ),
    stg_pedidos_lineas_pedidos as (
        select
            a.o_orderkey,
            a.o_clerk,
            b.l_orderkey,
            b.l_linenumber,
            a.o_origen,
            b.l_origen
        from {{ source("stg", "STG_PEDIDOS") }} a
        join {{ source("stg", "STG_LINEAS_PEDIDO") }} b
        where a.o_orderkey = b.l_orderkey
    )
select
    md5(
        upper(trim(nvl(o_orderkey, '')))
        || upper(trim(nvl(o_clerk, '')))
        || upper(trim(nvl(l_orderkey, '')))
        || upper(trim(nvl(l_linenumber, '')))
    ) as lnk_pedido_lin_pedido_id,
    p1.hub_pedido_id,
    p2.hub_lineas_pedido_id,
    p1.clave_pedido,
    p1.empleado,
    p2.linea_pedido,
    current_date as fecha_carga,
    p3.o_origen as origen_pedido,
    p3.l_origen as origen_linea_pedido
from hub_pedidos p1, hub_lineas_pedidos p2, stg_pedidos_lineas_pedidos p3
where
    (p1.clave_pedido = p3.o_orderkey and p1.empleado = p3.o_clerk)
    and (p1.clave_pedido = p3.l_orderkey and p2.linea_pedido = p3.l_linenumber)
