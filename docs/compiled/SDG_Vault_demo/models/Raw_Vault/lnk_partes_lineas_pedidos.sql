with
    hub_partes as (
        select hub_parte_id, nombre_parte, nombre_marca
        from SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_PARTES
    ),
    hub_lineas_pedidos as (
        select hub_lineas_pedido_id, clave_pedido, linea_pedido
        from SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_LINEAS_PEDIDOS
    ),
    stg_partes_lineas_pedidos as (
        select a.p_name, a.p_brand, b.l_orderkey, b.l_linenumber, a.p_origen, b.l_origen
        from SDGVAULTMART.DBT_SDGVAULT.STG_PARTES a
        join SDGVAULTMART.DBT_SDGVAULT.STG_LINEAS_PEDIDO b
        where a.p_partkey = b.l_partkey
    )
select
    md5(
        upper(trim(nvl(nombre_parte, '')))
        || upper(trim(nvl(nombre_marca, '')))
        || upper(trim(nvl(l_orderkey, '')))
        || upper(trim(nvl(l_linenumber, '')))
    ) as lnk_parte_lin_pedido_id,
    p1.hub_parte_id,
    p2.hub_lineas_pedido_id,
    p1.nombre_parte,
    p1.nombre_marca,
    p2.clave_pedido,
    p2.linea_pedido,
    current_date as fecha_carga,
    p3.p_origen as origen_parte,
    p3.l_origen as origen_linea_pedido
from hub_partes p1, hub_lineas_pedidos p2, stg_partes_lineas_pedidos p3
where
    (p1.nombre_parte = p3.p_name and p1.nombre_marca = p3.p_brand)
    and (p2.clave_pedido = p3.l_orderkey and p2.linea_pedido = p3.l_linenumber)