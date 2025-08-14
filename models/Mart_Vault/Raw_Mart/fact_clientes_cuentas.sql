{{ config(
    materialized = "incremental",
    unique_key = ["hub_cliente_id", "fecha"]
) }}

with
-- 1) Deduplicar PIT a una sola fila por cliente
pit_deduplicado as (
    select hub_cliente_id
    from {{ source("business", "PIT_CLIENTES") }}
    group by hub_cliente_id
),

-- 2) Obtener última fecha del SAT por cliente
max_fecha as (
    select hub_cliente_id, max(fecha_carga) as fecha_cliente_cuenta
    from {{ source("raw", "SAT_CLIENTES_CUENTA") }}
    group by hub_cliente_id
),

-- 3) Unir PIT con última fecha y datos del SAT
fact_clientes_cuentas as (
    select distinct
        pit.hub_cliente_id,
        sc.cuenta_balance,
        cast(sc.fecha_carga as date) as fecha,
        sc.c_origen as origen
    from pit_deduplicado pit
    join max_fecha mf
        on pit.hub_cliente_id = mf.hub_cliente_id
    left join {{ source("raw", "SAT_CLIENTES_CUENTA") }} sc
        on sc.hub_cliente_id = mf.hub_cliente_id
       and sc.fecha_carga   = mf.fecha_cliente_cuenta
)

-- 4) En incremental, solo insertar los que no existen ya
select *
from fact_clientes_cuentas f
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} t
    where f.hub_cliente_id = t.hub_cliente_id
      and f.fecha = t.fecha
)
{% endif %}
