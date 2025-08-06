-- Tabla PIT PIT_CLIENTES para la obtención eficiente del histórico persistido en las
-- tablas satelites de Cliente
{{
    config(
        materialized="incremental",
        unique_key="hub_cliente_id",
        merge_update_columns=[
            "pit_fecha",
            "fecha_cliente_contacto",
            "fecha_cliente_cuenta",
        ],
    )
}}

with
    sat_contacto as (
        select hub_cliente_id, max(fecha_carga) as fecha_cliente_contacto
        from {{ source("raw", "SAT_CLIENTES_CONTACTO") }}
        group by hub_cliente_id
    ),
    sat_cuenta as (
        select hub_cliente_id, max(fecha_carga) as fecha_cliente_cuenta
        from {{ source("raw", "SAT_CLIENTES_CUENTA") }}
        group by hub_cliente_id
    ),
    pit_clientes as (
        select
            h.hub_cliente_id,
            greatest(
                coalesce(fecha_cliente_contacto, date '1900-01-01'),
                coalesce(fecha_cliente_cuenta, date '1900-01-01')
            ) as pit_fecha,
            s1.fecha_cliente_contacto,
            s2.fecha_cliente_cuenta
        from {{ source("raw", "HUB_CLIENTES") }} h
        left join sat_contacto s1 on h.hub_cliente_id = s1.hub_cliente_id
        left join sat_cuenta s2 on h.hub_cliente_id = s2.hub_cliente_id
    ),

    filtrado as (

        {% if is_incremental() %}

            select p.*
            from pit_clientes p
            left join
                {{ this }} t
                on p.hub_cliente_id = t.hub_cliente_id
                and p.fecha_cliente_contacto = t.fecha_cliente_contacto
                and p.fecha_cliente_cuenta = t.fecha_cliente_cuenta
            where t.hub_cliente_id is null

        {% else %}select * from pit_clientes

        {% endif %}

    )
select *
from filtrado
