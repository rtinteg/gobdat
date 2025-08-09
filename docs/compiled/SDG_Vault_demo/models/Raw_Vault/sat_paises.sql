

with
    sat_paises as (
        select
            b.hub_pais_id,
            b.fecha_carga,
            md5(upper(trim(nvl(n_comment, '')))) as foto_pais,
            n_origen,
            n_comment as comentario
        from SDGVAULTMART.DBT_SDGVAULT.STG_PAISES a
        join SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_PAISES b on a.n_name = b.nombre_pais

        
            where
                not exists (
                    select 1
                    from SDGVAULTMART.DBT_SDGVAULT_BRONZE.sat_paises s
                    where
                        s.hub_pais_id = b.hub_pais_id
                        and s.foto_pais = md5(upper(trim(nvl(n_comment, ''))))
                )
        
    )
select *
from sat_paises