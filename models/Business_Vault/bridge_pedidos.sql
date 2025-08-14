{{
    config(
        materialized="incremental",
        unique_key="bridge_pedido_id",
        on_schema_change="sync_all_columns",
    )
}}

with
    -- 1) Deduplicar Links (última fila por par lógico)
    lcp as (
        select *
        from {{ source("raw", "LNK_CLIENTES_PEDIDOS") }}
        qualify
            row_number() over (
                partition by hub_pedido_id, hub_cliente_id order by fecha_carga desc
            )
            = 1
    ),
    lpc as (
        select *
        from {{ source("raw", "LNK_PAISES_CLIENTES") }}
        qualify
            row_number() over (
                partition by hub_cliente_id, hub_pais_id order by fecha_carga desc
            )
            = 1
    ),

    -- 2) Base sin multiplicidades
    base as (
        select
            /* Clave BRIDGE = MD5 de textos de negocio (con separador para evitar colisiones) */
            md5(
                upper(trim(coalesce(hp.clave_pedido, '')))
                || '|'
                || upper(trim(coalesce(hp.empleado, '')))
                || '|'
                || upper(trim(coalesce(hc.nombre_cliente, '')))
                || '|'
                || upper(trim(coalesce(hn.nombre_pais, '')))
            ) as bridge_pedido_id,

            greatest(lcp.fecha_carga, lpc.fecha_carga) as fecha_carga,
            hp.hub_pedido_id,
            hp.clave_pedido,
            hp.empleado,
            lcp.lnk_cliente_pedido_id,
            hc.hub_cliente_id,
            hc.nombre_cliente,
            lpc.lnk_pais_cliente_id,
            hn.hub_pais_id,
            hn.nombre_pais
        from {{ source("raw", "HUB_PEDIDOS") }} hp
        join lcp on lcp.hub_pedido_id = hp.hub_pedido_id
        join
            {{ source("raw", "HUB_CLIENTES") }} hc
            on hc.hub_cliente_id = lcp.hub_cliente_id
        join lpc on lpc.hub_cliente_id = hc.hub_cliente_id
        join {{ source("raw", "HUB_PAISES") }} hn on hn.hub_pais_id = lpc.hub_pais_id

        {% if is_incremental() %}
            -- 3) Watermark incremental: solo filas nuevas respecto al destino
            where
                greatest(lcp.fecha_carga, lpc.fecha_carga) > (
                    select coalesce(max(fecha_carga), to_timestamp_ntz('1900-01-01'))
                    from {{ this }}
                )
        {% endif %}
    ),

    -- 4) Deduplicar dentro del lote por clave de Bridge
    dedup_lote as (
        select
            b.*,
            row_number() over (
                partition by bridge_pedido_id order by fecha_carga desc
            ) as rn
        from base b
    )

select
    bridge_pedido_id,
    fecha_carga,
    hub_pedido_id,
    clave_pedido,
    empleado,
    lnk_cliente_pedido_id,
    hub_cliente_id,
    nombre_cliente,
    lnk_pais_cliente_id,
    hub_pais_id,
    nombre_pais
from dedup_lote
where rn = 1
