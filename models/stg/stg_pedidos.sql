{{ config(materialized="incremental", unique_key="o_orderkey") }}

with
    source_data as (
        {% if is_incremental() %}
            select
                o_orderkey,
                o_custkey,
                o_orderstatus,
                o_totalprice,
                o_orderdate,
                o_orderpriority,
                o_clerk,
                o_shippriority,
                o_comment,
                o_origen
            from {{ source("stg", "PEDIDOS_ELT") }}
            where o_orderkey not in (select o_orderkey from {{ this }})
        {% else %}
            select
                o_orderkey,
                o_custkey,
                o_orderstatus,
                o_totalprice,
                o_orderdate,
                o_orderpriority,
                o_clerk,
                o_shippriority,
                o_comment,
                'Snowflake' as o_origen
            from {{ source("src", "ORDERS") }}
        {% endif %}
    )
select *
from source_data
