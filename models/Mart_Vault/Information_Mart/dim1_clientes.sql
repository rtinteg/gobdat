{{
    config(
        materialized="incremental",
        unique_key="dim1_cliente_id",
        merge_update_columns=["nombre_cliente", "segmento_marketing", "fecha"],
    )
}}

-- 1) Elegimos UNA fila del PIT por cliente (la más reciente)
with
    pit_clientes as (
        select
            pit.*,
            row_number() over (
                partition by pit.hub_cliente_id order by pit.fecha_cliente_cuenta desc
            ) as rn_pit
        from {{ source("business", "PIT_CLIENTES") }} pit
        qualify rn_pit = 1
    ),

    -- 2) Para cada cliente, buscamos el SAT más cercano por debajo/igual a la fecha
    -- del PIT
    sat_clientes_cuenta as (
        select
            s.hub_cliente_id,
            s.segmento_marketing,
            s.fecha_carga,
            s.c_origen as origen,
            row_number() over (
                partition by s.hub_cliente_id order by s.fecha_carga desc
            ) as rn_latest
        from {{ source("raw", "SAT_CLIENTES_CUENTA") }} s
    ),

    -- 2.b) Elegimos la fila del SAT con fecha <= PIT; si no existiera, caemos al
    -- último SAT disponible
    sat_pit as (
        -- preferimos fecha <= pit.fecha_cliente_cuenta
        select
            p.hub_cliente_id,
            s.segmento_marketing,
            s.fecha_carga,
            s.origen,
            row_number() over (
                partition by p.hub_cliente_id order by s.fecha_carga desc
            ) as rn_pick
        from pit_clientes p
        join
            sat_clientes_cuenta s
            on s.hub_cliente_id = p.hub_cliente_id
            and s.fecha_carga <= p.fecha_cliente_cuenta
        qualify rn_pick = 1

        union all

        -- fallback: si no hubo ninguna <=, toma el SAT más reciente (evita NULLs)
        select
            p.hub_cliente_id,
            s.segmento_marketing,
            s.fecha_carga,
            s.origen,
            1 as rn_pick
        from pit_clientes p
        join sat_clientes_cuenta s on s.hub_cliente_id = p.hub_cliente_id
        qualify
            not exists (
                select 1
                from sat_clientes_cuenta s2
                where
                    s2.hub_cliente_id = p.hub_cliente_id
                    and s2.fecha_carga <= p.fecha_cliente_cuenta
            )
            and s.rn_latest = 1
    ),

    dim1_clientes as (
        select
            hc.hub_cliente_id as dim1_cliente_id,
            hc.nombre_cliente,
            s.segmento_marketing,
            s.fecha_carga as fecha,
            s.origen
        from {{ source("raw", "HUB_CLIENTES") }} hc
        join pit_clientes p on hc.hub_cliente_id = p.hub_cliente_id
        join sat_pit s on s.hub_cliente_id = p.hub_cliente_id
    )

-- 3) Garantizamos UNA fila por clave (defensa extra)
select dim1_cliente_id, nombre_cliente, segmento_marketing, fecha, origen
from
    (
        select
            d.*,
            row_number() over (
                partition by dim1_cliente_id order by fecha desc
            ) as rn_final
        from dim1_clientes d
    )
qualify rn_final = 1
