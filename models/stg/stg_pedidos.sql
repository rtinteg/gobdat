with stg_pedidos as (select * from {{ source("src", "ORDERS") }})
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
from stg_pedidos
