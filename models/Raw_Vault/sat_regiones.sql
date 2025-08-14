with
    sat_regiones as (
        select
            b.hub_region_id,
            current_date as fecha_carga,
            md5(
                upper(trim(nvl(r_comment, ''))) || upper(trim(nvl(r_origen, '')))
            ) as foto_cliente,
            a.r_origen,
            a.r_comment as comentario,
        from {{ source("stg", "STG_REGIONES") }} a
        join {{ source("raw", "HUB_REGIONES") }} b on a.r_name = b.nombre_region
    )
select *
from sat_regiones
