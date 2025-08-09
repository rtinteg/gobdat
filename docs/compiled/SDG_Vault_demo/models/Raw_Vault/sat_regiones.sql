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
        from SDGVAULTMART.DBT_SDGVAULT.STG_REGIONES a
        join SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_REGIONES b on a.r_name = b.nombre_region
    )
select *
from sat_regiones