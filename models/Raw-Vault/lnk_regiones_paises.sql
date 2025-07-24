with
    stg_regiones as (
        select md5(upper(trim(nvl(r_name, '')))) as region_id, r_name as nombre_region,
        from {{ source("stg", "STG_REGIONES") }}
    ),
    stg_paises as (
        select md5(upper(trim(nvl(n_name, '')))) as hub_pais_id, n_name as nombre_pais,
        from {{ source("stg", "STG_PAISES") }}
    )
select
    md5(
        upper(trim(nvl(p1.nombre_region, ''))) || upper(trim(nvl(p2.nombre_pais, '')))
    ) as lnk_regiones_paises_id,
    p1.nombre_region,
    p2.nombre_pais,
    current_date as fecha_carga,
    'Snowflake' as origen
from stg_regiones p1, stg_paises p2
