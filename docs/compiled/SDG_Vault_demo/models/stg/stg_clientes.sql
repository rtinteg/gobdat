

-- Fuente condicional de datos según tipo de carga
with
    source_data as (

        

            -- Incremental: datos nuevos desde CSV
            select
                c_acctbal,
                c_address,
                c_comment,
                c_custkey,
                c_mktsegment,
                c_name,
                c_nationkey,
                c_phone,
                'CSV' as c_origen,
                current_date - 2 as load_date
            from SDGVAULTMART.DBT_SDGVAULT.CLIENTES_ELT

        

    )

select *
from source_data