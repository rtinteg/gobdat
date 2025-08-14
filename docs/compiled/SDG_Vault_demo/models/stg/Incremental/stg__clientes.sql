
with
    csv_nuevos as (
        select
            c_acctbal,
            c_address,
            c_comment,
            c_custkey,
            c_mktsegment,
            c_name,
            c_nationkey,
            c_phone,
            c_origen
        from SDGVAULTMART.DBT_SDGVAULT.CLIENTES_ELT
    ),
    filtrados as (

        select *
        from csv_nuevos
        where c_custkey not in (select c_custkey from SDGVAULTMART.DBT_SDGVAULT.stg_clientes)

    )
select *
from filtrados