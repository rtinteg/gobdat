with
    hub_lineas_pedidos as (
        select
            md5(
                upper(trim(nvl(l_orderkey, ''))) || upper(trim(nvl(l_linenumber, '')))
            ) as hub_lineas_pedido_id,
            l_orderkey as clave_pedido,
            l_linenumber as linea_pedido,
            current_date as fecha_carga,
            l_origen as origen
        from SDGVAULTMART.DBT_SDGVAULT.STG_LINEAS_PEDIDO
    )
select *
from hub_lineas_pedidos