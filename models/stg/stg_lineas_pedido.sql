-- {{ config(materialized="table") }}
with stg_lineas_pedido as (select * from {{ source("src", "LINEITEM") }})
select
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode,
    l_comment,
    'Snowflake' as l_origen
from stg_lineas_pedido
