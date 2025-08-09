

with
    base as (
        select
            bp.hub_pedido_id,
            bp.hub_cliente_id,
            bp.hub_pais_id,
            bp.fecha_carga,
            lp.linea_pedido,
            sp.precio_total
        from SDGVAULTMART.DBT_SDGVAULT_BRONZE.BRIDGE_PEDIDOS bp
        join
            SDGVAULTMART.DBT_SDGVAULT_BRONZE.SAT_PEDIDOS sp on bp.hub_pedido_id = sp.hub_pedido_id
        LEFT join
            SDGVAULTMART.DBT_SDGVAULT_BRONZE.LNK_PEDIDOS_LINEAS_PEDIDOS lp
            on bp.hub_pedido_id = lp.hub_pedido_id

        
            -- Solo considerar nuevas fechas de carga en modo incremental
            where bp.fecha_carga > (select max(fecha_carga) from SDGVAULTMART.DBT_SDGVAULT_GOLD.fact_clientes_pedidos)
        
    ),
    agregado as (
        select
            hub_pedido_id,
            hub_cliente_id,
            hub_pais_id,
            fecha_carga,
            sum(precio_total) as precio_total_acumulado,
            sum(linea_pedido) as cantidad_lineas_pedido
        from base
        group by hub_pedido_id, hub_cliente_id, hub_pais_id, fecha_carga
    )
select *
from agregado
--ORDER BY fecha_carga DESC