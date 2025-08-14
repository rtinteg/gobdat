
with
    csv_nuevos as (select * from SDGVAULTMART.DBT_SDGVAULT.PAISES_ELT),
    filtrados as (
        select *
        from csv_nuevos
        where n_nationkey not in (select n_nationkey from SDGVAULTMART.DBT_SDGVAULT.stg_paises)
    )
select *
from filtrados