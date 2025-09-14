

with
    v_hub_sat_pedidos_eu as (
        select
            hpe.hub_pedido_id,
            hpe.clave_pedido as hub_clave_pedido,
            hpe.empleado as hub_empleado,
            hpe.fecha_carga as hub_fecha_carga,
            hpe.origen as hub_origen,
            spe.fecha_carga as sat_fecha_carga,
            spe.foto_pedido as sat_foto_pedido,
            spe.o_origen as sat_origen,
            spe.estado_pedido as sat_estado_pedido,
            spe.precio_total as sat_precio_total,
            spe.fecha_pedido as sat_fecha_pedido,
            spe.prioridad_env as sat_prioridad_env,
            spe.prioridad_ped as sat_prioridad_ped,
            spe.comentario as sat_comentario
        from SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_PEDIDOS hpe
        join
            SDGVAULTMART.DBT_SDGVAULT_BRONZE.SAT_PEDIDOS spe
            on hpe.hub_pedido_id = spe.hub_pedido_id
        join
            SDGVAULTMART.DBT_SDGVAULT_BRONZE.LNK_CLIENTES_PEDIDOS lcp
            on hpe.hub_pedido_id = lcp.hub_pedido_id
        join
            SDGVAULTMART.DBT_SDGVAULT_BRONZE.LNK_PAISES_CLIENTES lpc
            on lcp.hub_cliente_id = lpc.hub_cliente_id
            and lpc.nombre_pais
            in ('ESPAÑA', 'GERMANY', 'RUSSIA', 'ROMANIA', 'UNITED KINGDOM', 'FRANCE')
    )
select *
from v_hub_sat_pedidos_eu