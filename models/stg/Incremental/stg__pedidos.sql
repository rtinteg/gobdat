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

    {% if is_incremental() %}
        filtrados as (
            select n.*
            from csv_nuevos n
            left join {{ this }} e on n.o_orderkey = e.o_orderkey
            where e.o_orderkey is null
        )
    {% else %} filtrados as (select * from csv_nuevos)
    {% endif %}

select *
from
    filtrados

    -- with
    -- csv_nuevos as (select * from {{ source("stg", "PEDIDOS_ELT") }}),
    -- filtrados as (
    -- select n.*
    -- from csv_nuevos n
    -- left join {{ ref("stg_pedidos") }} e
    -- on n.o_orderkey = e.o_orderkey
    -- where e.o_orderkey is null
    -- where o_orderkey not in (select o_orderkey from {{ ref("stg_pedidos") }})
    -- )
    -- select *
    -- from filtrados
    
