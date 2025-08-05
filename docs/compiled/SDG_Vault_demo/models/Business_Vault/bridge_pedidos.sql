

with
    bridge_pedido as (
        select
            md5(
                upper(trim(nvl(hp.clave_pedido, '')))
                || upper(trim(nvl(hp.empleado, '')))
                || upper(trim(nvl(hc.nombre_cliente, '')))
                || upper(trim(nvl(hn.nombre_pais, '')))
            ) as bridge_pedido_id,
            current_date fecha_carga,
            hp.hub_pedido_id,
            hp.clave_pedido,
            hp.empleado,
            lcp.lnk_cliente_pedido_id,
            hc.hub_cliente_id,
            hc.nombre_cliente,
            lpc.lnk_pais_cliente_id,
            hn.hub_pais_id,
            hn.nombre_pais
        from SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_PEDIDOS hp
        join
            SDGVAULTMART.DBT_SDGVAULT_BRONZE.LNK_CLIENTES_PEDIDOS lcp
            on hp.hub_pedido_id = lcp.hub_pedido_id
        join
            SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_CLIENTES hc
            on hc.hub_cliente_id = lcp.hub_cliente_id
        join
            SDGVAULTMART.DBT_SDGVAULT_BRONZE.LNK_PAISES_CLIENTES lpc
            on lpc.hub_cliente_id = hc.hub_cliente_id
        join SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_PAISES hn on hn.hub_pais_id = lpc.hub_pais_id
    )
select *
from bridge_pedido


    where bridge_pedido_id not in (select bridge_pedido_id from SDGVAULTMART.DBT_SDGVAULT_BRONZE.bridge_pedidos)
