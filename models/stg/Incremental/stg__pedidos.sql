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
    filtrados as (
        select
            n.o_orderkey,
            n.o_custkey,
            n.o_orderstatus,
            n.o_totalprice,
            n.o_orderdate,
            n.o_orderpriority,
            n.o_clerk,
            n.o_shippriority,
            n.o_comment,
            n.o_origen
        from csv_nuevos n
        where
            (n.o_orderkey, n.o_custkey)
            not in (select o_orderkey, o_custkey from {{ ref("stg_pedidos") }})
    )
select *
from filtrados
