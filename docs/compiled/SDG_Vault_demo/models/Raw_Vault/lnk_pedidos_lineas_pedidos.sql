with
    hub_pedidos as (
        select hub_pedido_id, clave_pedido, empleado
        from SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_PEDIDOS
    ),
    hub_lineas_pedidos as (
        select hub_lineas_pedido_id, clave_pedido, linea_pedido
        from SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_LINEAS_PEDIDOS
    ),
    stg_pedidos_lineas_pedidos as (
        select
            a.o_orderkey,
            a.o_clerk,
            b.l_orderkey,
            b.l_linenumber,
            a.o_origen,
            b.l_origen
        from SDGVAULTMART.DBT_SDGVAULT.STG_PEDIDOS a
        join SDGVAULTMART.DBT_SDGVAULT.STG_LINEAS_PEDIDO b
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
    and (p2.clave_pedido = p3.l_orderkey and p2.linea_pedido = p3.l_linenumber)