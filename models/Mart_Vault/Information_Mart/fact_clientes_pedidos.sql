{{
    config(
        materialized="incremental",
        unique_key=["hub_pedido_id", "hub_cliente_id", "hub_pais_id", "fecha_carga"],
    )
}}

with
    base as (
        select
            bp.hub_pedido_id,
            bp.hub_cliente_id,
            bp.hub_pais_id,
            bp.fecha_carga,
            lp.linea_pedido,
            sp.precio_total
        from {{ source("business", "BRIDGE_PEDIDOS") }} bp
        join
            {{ source("raw", "SAT_PEDIDOS") }} sp on bp.hub_pedido_id = sp.hub_pedido_id
        LEFT join
            {{ source("raw", "LNK_PEDIDOS_LINEAS_PEDIDOS") }} lp
            on bp.hub_pedido_id = lp.hub_pedido_id

        {% if is_incremental() %}
            -- Solo considerar nuevas fechas de carga en modo incremental
            where bp.fecha_carga > (select max(fecha_carga) from {{ this }})
        {% endif %}
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