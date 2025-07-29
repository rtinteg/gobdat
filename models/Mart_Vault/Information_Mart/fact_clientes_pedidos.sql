with
    base as (
        select
            bp.hub_pedido_id,
            bp.hub_cliente_id,
            bp.hub_pais_id,
            bp.fecha_carga,
            sp.precio_total
        from {{ source("business", "BRIDGE_PEDIDOS") }} bp
        join
            {{ source("raw", "SAT_PEDIDOS") }} sp on bp.hub_pedido_id = sp.hub_pedido_id
    ),
    agregado as (
        select
            hub_cliente_id,
            hub_pais_id,
            fecha_carga,
            avg(precio_total) as precio_total_medio,
            sum(precio_total) as precio_total_acumulado
        from base
        group by hub_cliente_id, hub_pais_id, fecha_carga
    )
select *
from agregado
