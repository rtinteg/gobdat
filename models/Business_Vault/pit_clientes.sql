{{
    config(
        materialized="incremental",
        unique_key=["hub_cliente_id", "pit_fecha"],
        merge_update_columns=["fecha_cliente_contacto", "fecha_cliente_cuenta"],
    )
}}

with
    fechas_contacto as (
        select distinct hub_cliente_id, fecha_carga as fecha_cliente_contacto
        from {{ source("raw", "SAT_CLIENTES_CONTACTO") }}
    ),
    fechas_cuenta as (
        select distinct hub_cliente_id, fecha_carga as fecha_cliente_cuenta
        from {{ source("raw", "SAT_CLIENTES_CUENTA") }}
    ),
    combinaciones as (
        select
            coalesce(fc.hub_cliente_id, fa.hub_cliente_id) as hub_cliente_id,
            fc.fecha_cliente_contacto,
            fa.fecha_cliente_cuenta,
            greatest(
                coalesce(fc.fecha_cliente_contacto, date '1900-01-01'),
                coalesce(fa.fecha_cliente_cuenta, date '1900-01-01')
            ) as pit_fecha
        from fechas_contacto fc
        full outer join fechas_cuenta fa on fc.hub_cliente_id = fa.hub_cliente_id
    ),
    pit_clientes as (
        {% if is_incremental() %}
            select c.*
            from combinaciones c
            left join
                {{ this }} t
                on c.hub_cliente_id = t.hub_cliente_id
                and c.fecha_cliente_contacto = t.fecha_cliente_contacto
                and c.fecha_cliente_cuenta = t.fecha_cliente_cuenta
            where t.hub_cliente_id is null
        {% else %}select * from combinaciones
        {% endif %}
    )

select *
from pit_clientes

