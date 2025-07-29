with
    hub_regiones as (
        select hub_region_id, nombre_region from {{ source("raw", "HUB_REGIONES") }}
    ),
    hub_paises as (
        select hub_pais_id, nombre_pais, from {{ source("raw", "HUB_PAISES") }}
    ),
    stg_regiones_paises as (
        select a.n_name, b.r_name, a.n_origen, b.r_origen
        from {{ source("stg", "STG_PAISES") }} a
        join {{ source("stg", "STG_REGIONES") }} b
        where a.n_regionkey = b.r_regionkey
    )
select
    md5(
        upper(trim(nvl(p1.nombre_region, ''))) || upper(trim(nvl(p2.nombre_pais, '')))
    ) as lnk_regiones_paises_id,
    p1.hub_region_id,
    p2.hub_pais_id,
    p1.nombre_region,
    p2.nombre_pais,
    current_date as fecha_carga,
    p3.r_origen as origen_region,
    p3.n_origen as origen_pais
from hub_regiones p1, hub_paises p2, stg_regiones_paises p3
where p1.nombre_region = p3.r_name and p2.nombre_pais = p3.n_name
