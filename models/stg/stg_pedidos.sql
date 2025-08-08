{{ config(materialized="incremental", unique_key=["o_orderkey", "load_date"]) }}

with
    source_data as (

        {% if is_incremental() %}

            -- Incremental: carga desde CSV
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
                'CSV' as o_origen,
                current_date + 1 as load_date
            from {{ source("stg", "PEDIDOS_ELT") }}

        {% else %}

            -- Primera carga: desde tabla fuente sin campos extra
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
                'Snowflake' as o_origen,
                current_date - 15 as load_date
            from {{ source("src", "ORDERS") }}

        {% endif %}

    )

select *
from source_data
