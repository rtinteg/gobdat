{{
    config(
        materialized="incremental",
        unique_key="o_orderkey",
        schema="DBT_SDGVAULT",
        database="SDGVAULTMART",
        alias="STG_PEDIDOS",
    )
}}
with
    csv_nuevos as (select * from {{ source("stg", "PEDIDOS_ELT") }}),
    filtrados as (
        select *
        from csv_nuevos
        where o_orderkey not in (select o_orderkey from {{ ref("STG_PEDIDOS") }})
    )
select *
from filtrados
