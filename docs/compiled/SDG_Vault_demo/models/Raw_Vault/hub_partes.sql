with
    hub_partes as (
        select
            md5(
                upper(trim(nvl(p_name, ''))) || upper(trim(nvl(p_brand, '')))
            ) as hub_parte_id,
            p_name as nombre_parte,
            p_brand as nombre_marca,
            current_date as fecha_carga,
            p_origen as origen
        from SDGVAULTMART.DBT_SDGVAULT.STG_PARTES
    )
select *
from hub_partes