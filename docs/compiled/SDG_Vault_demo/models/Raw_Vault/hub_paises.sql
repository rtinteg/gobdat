

with
    hub_paises as (
        
            select
                md5(upper(trim(nvl(n_name, '')))) as hub_pais_id,
                n_name as nombre_pais,
                current_date as fecha_carga,
                n_origen as origen
            from SDGVAULTMART.DBT_SDGVAULT.STG_PAISES
            where n_name not in (select nombre_pais from SDGVAULTMART.DBT_SDGVAULT_BRONZE.hub_paises)
        
    )
select *
from hub_paises