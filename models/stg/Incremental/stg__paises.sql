{{
    config(
        materialized="incremental",
        unique_key="n_nationkey",
        database="SDGVAULTMART",
        alias="stg_paises",
    )
}}
with
    csv_nuevos as (select * from {{ source("stg", "PAISES_ELT") }}),
    filtrados as (
        select *
        from csv_nuevos
        where n_nationkey not in (select n_nationkey from {{ ref("stg_paises") }})
    )
select *
from filtrados
