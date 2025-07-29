{{
    config(
        materialized="incremental",
        unique_key=["o_orderkey", "o_custkey"],
        schema="DBT_SDGVAULT",
        database="SDGVAULTMART",
        identifier="stg_pedidos",
    )
}}

with
    csv_nuevos as (
        select
            c.o_orderkey,
            c.o_custkey,
            c.o_orderstatus,
            c.o_totalprice,
            c.o_orderdate,
            c.o_orderpriority,
            c.o_clerk,
            c.o_shippriority,
            c.o_comment,
            c.o_origen
        from {{ source("stg", "PEDIDOS_ELT") }} c
    ),

    {% if is_incremental() %}
        filtrados as (
            select n.*
            from csv_nuevos n
            left join
                {{ this }} e
                on n.o_orderkey = e.o_orderkey
                and n.o_custkey = e.o_custkey
            where e.o_orderkey is null and e.o_custkey is null
        )
    {% else %} filtrados as (select * from csv_nuevos)
    {% endif %}

select *
from filtrados
