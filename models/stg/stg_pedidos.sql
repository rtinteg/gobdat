-- {{ config(materialized="table") }}

with stg_pedidos as (select * from {{ source("stg", "ORDERS") }})
select *
from stg_pedidos
