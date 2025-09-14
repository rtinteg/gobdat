

with
    hub_pedidos as (
        
            select
                md5(
                    upper(trim(nvl(o_orderkey, ''))) || upper(trim(nvl(o_clerk, '')))
                ) as hub_pedido_id,
                o_orderkey as clave_pedido,
                o_clerk as empleado,
                load_date as fecha_carga,
                o_origen as origen
            from SDGVAULTMART.DBT_SDGVAULT.STG_PEDIDOS
            where
                (o_orderkey, o_clerk)
                not in (select clave_pedido, empleado from SDGVAULTMART.DBT_SDGVAULT_BRONZE.hub_pedidos)
        
    )
select *
from hub_pedidos