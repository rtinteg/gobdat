{{
    config(
        materialized="incremental",
        unique_key="c_custkey",
        schema="DBT_SDGVAULT",
        database="SDGVAULTMART",
        alias="stg_clientes",
    )
}}
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
        from {{ source("stg", "CLIENTES_ELT") }}
    ),
    filtrados as (

        select *
        from csv_nuevos
        where c_custkey not in (select c_custkey from {{ ref("stg_clientes") }})

    )
select *
from filtrados
