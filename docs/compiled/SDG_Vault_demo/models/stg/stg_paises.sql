

-- Selección condicional de la fuente de datos
with
    source_data as (

        
            select n_nationkey, n_name, n_regionkey, n_comment, n_origen
            from SDGVAULTMART.DBT_SDGVAULT.PAISES_ELT
            where n_nationkey not in (select n_nationkey from SDGVAULTMART.DBT_SDGVAULT.stg_paises)
        

    )
select *
from source_data