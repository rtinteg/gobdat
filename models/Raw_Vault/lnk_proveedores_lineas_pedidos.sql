with
    hub_proveedores as (
        select hub_proveedor_id, nombre_proveedor
        from {{ source("raw", "HUB_PROVEEDORES") }}
    ),
    hub_lineas_pedidos as (
        select hub_lineas_pedido_id, clave_pedido, linea_pedido
        from {{ source("raw", "HUB_LINEAS_PEDIDOS") }}
    ),
    stg_proveedores_lineas_pedidos as (
        select a.s_name, b.l_orderkey, b.l_linenumber, a.s_origen, b.l_origen
        from {{ source("stg", "STG_PROVEEDORES") }} a
        join {{ source("stg", "STG_LINEAS_PEDIDO") }} b
        where a.s_suppkey = b.l_suppkey
    )
select
    md5(
        upper(trim(nvl(nombre_proveedor, '')))
        || upper(trim(nvl(l_orderkey, '')))
        || upper(trim(nvl(l_linenumber, '')))
    ) as lnk_proveedor_lin_pedido_id,
    p1.hub_proveedor_id,
    p2.hub_lineas_pedido_id,
    p1.nombre_proveedor,
    p2.clave_pedido,
    p2.linea_pedido,
    current_date as fecha_carga,
    p3.s_origen as origen_proveedor,
    p3.l_origen as origen_linea_pedido
from hub_proveedores p1, hub_lineas_pedidos p2, stg_proveedores_lineas_pedidos p3
where
    p1.nombre_proveedor = p3.s_name
    and (p2.clave_pedido = p3.l_orderkey and p2.linea_pedido = p3.l_linenumber)
