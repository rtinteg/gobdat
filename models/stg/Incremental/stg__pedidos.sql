{{
    config(
        materialized="incremental",
        unique_key="o_orderkey",
        schema="DBT_SDGVAULT",
        database="SDGVAULTMART",
        alias="stg_pedidos",
    )
}}
with
    csv_nuevos as (select * from {{ source("stg", "PEDIDOS_ELT") }}),
    filtrados as (
        select *
        from csv_nuevos
        where o_orderkey not in (select o_orderkey from {{ ref("stg_pedidos") }})
    )
select *
from filtrados
