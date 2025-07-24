with
    hub_regiones as (
        select hub_region_id, nombre_region from {{ source("raw", "HUB_REGIONES") }}
    ),
    hub_paises as (
        select hub_pais_id, nombre_pais, from {{ source("raw", "HUB_PAISES") }}
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
    'Snowflake' as origen
from hub_regiones p1, hub_paises p2
