

with
    dim1_clientes as (
        select
            hc.hub_cliente_id as dim1_cliente_id,
            hc.nombre_cliente,
            sc.segmento_marketing,
            sc.fecha_carga as fecha,
            sc.c_origen as origen
        from SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_CLIENTES hc
        join
            SDGVAULTMART.DBT_SDGVAULT_BRONZE.PIT_CLIENTES pit
            on hc.hub_cliente_id = pit.hub_cliente_id
        left join
            SDGVAULTMART.DBT_SDGVAULT_BRONZE.SAT_CLIENTES_CUENTA sc
            on sc.hub_cliente_id = pit.hub_cliente_id
            and sc.fecha_carga = pit.fecha_cliente_cuenta
            and sc.fecha_carga = (
                select max(sc2.fecha_carga)
                from SDGVAULTMART.DBT_SDGVAULT_BRONZE.SAT_CLIENTES_CUENTA sc2
                where sc.hub_cliente_id = sc2.hub_cliente_id
            )
    ),
    filtrado as (
        select s.*
        from dim1_clientes s
        left join
            SDGVAULTMART.DBT_SDGVAULT_GOLD.dim1_clientes t
            on s.dim1_cliente_id = t.dim1_cliente_id
            and s.nombre_cliente = t.nombre_cliente
            and s.segmento_marketing = t.segmento_marketing
            and s.fecha = t.fecha
            and s.origen = t.origen
        where t.dim1_cliente_id is null
    )
select *
from filtrado