with
    hub_regiones as (
        select
            md5(upper(trim(nvl(r_name, '')))) as hub_region_id,
            r_name as nombre_region,
            current_date as fecha_carga,
            r_origen as origen
        from {{ source("stg", "STG_REGIONES") }}
    )
select *
from hub_regiones
