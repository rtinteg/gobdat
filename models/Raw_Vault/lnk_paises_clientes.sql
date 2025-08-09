{{ config(materialized="incremental", unique_key="lnk_pais_cliente_id") }}

with
    hub_paises as (
        select hub_pais_id, nombre_pais from {{ source("raw", "HUB_PAISES") }}
    ),
    hub_clientes as (
        select hub_cliente_id, nombre_cliente, fecha_carga
        from {{ source("raw", "HUB_CLIENTES") }}
    ),
    stg_paises_clientes as (
        select a.n_name, b.c_name, a.n_origen, b.c_origen, b.load_date
        from {{ source("stg", "STG_PAISES") }} a
        join {{ source("stg", "STG_CLIENTES") }} b
        where a.n_nationkey = b.c_nationkey
    ),
    combinaciones as (
        select
            md5(
                upper(trim(nvl(p1.nombre_pais, '')))
                || upper(trim(nvl(p2.nombre_cliente, '')))
            ) as lnk_pais_cliente_id,
            p1.hub_pais_id,
            p2.hub_cliente_id,
            p1.nombre_pais,
            p2.nombre_cliente,
            p3.load_date as fecha_carga,
            p3.n_origen as origen_pais,
            p3.c_origen as origen_cliente
        from hub_paises p1, hub_clientes p2, stg_paises_clientes p3
        where p1.nombre_pais = p3.n_name and p2.nombre_cliente = p3.c_name
    )

select *
from combinaciones

{% if is_incremental() %}
    where lnk_pais_cliente_id not in (select lnk_pais_cliente_id from {{ this }})
{% endif %}
