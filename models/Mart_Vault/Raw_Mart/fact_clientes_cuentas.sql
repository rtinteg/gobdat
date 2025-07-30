{{ config(materialized="incremental", unique_key=["hub_cliente_id", "fecha"]) }}

with
    max_fecha as (
        select hub_cliente_id, max(fecha_carga) as fecha_cliente_cuenta
        from {{ source("raw", "SAT_CLIENTES_CUENTA") }}
        group by hub_cliente_id
    ),
    pit_enriquecido as (
        select pit.hub_cliente_id, mf.fecha_cliente_cuenta
        from {{ source("business", "PIT_CLIENTES") }} pit
        join max_fecha mf on pit.hub_cliente_id = mf.hub_cliente_id
    ),
    fact_clientes_cuentas as (
        select
            pit.hub_cliente_id,
            sc.cuenta_balance,
            sc.fecha_carga as fecha,
            sc.c_origen as origen
        from pit_enriquecido pit
        left join
            {{ source("raw", "SAT_CLIENTES_CUENTA") }} sc
            on sc.hub_cliente_id = pit.hub_cliente_id
            and sc.fecha_carga = pit.fecha_cliente_cuenta
    )
-- Solo inserta registros que no existen aún en la tabla de hechos
select *
from fact_clientes_cuentas f
{% if is_incremental() %}
    where
        not exists (
            select 1
            from {{ this }} t
            where f.hub_cliente_id = t.hub_cliente_id and f.fecha = t.fecha
        )
{% endif %}
