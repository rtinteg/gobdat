

with
    sat_paises as (
        
            select
                hub_pais_id,
                current_date as fecha_carga,
                md5(
                    upper(trim(nvl(n_comment, ''))) || upper(trim(nvl(n_origen, '')))
                ) as foto_pais,
                n_origen,
                n_comment as comentario,
            from SDGVAULTMART.DBT_SDGVAULT.STG_PAISES a
            join SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_PAISES b on a.n_name = b.nombre_pais 
            where hub_pais_id not in (select hub_pais_id from SDGVAULTMART.DBT_SDGVAULT_BRONZE.sat_paises)
        
    )
select *
from sat_paises