

-- Fuente condicional de datos según tipo de carga
with
    source_data as (
        

            -- Incremental: datos nuevos desde CSV
            select
                n_nationkey,
                n_name,
                n_regionkey,
                n_comment,
                n_origen,
                current_date - 2 as load_date
            from SDGVAULTMART.DBT_SDGVAULT.PAISES_ELT

        
    )

select *
from source_data