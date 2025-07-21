-- {{ config(materialized="table") }}

with stg_lineas_pedido as (select * from {{ source("stg", "LINEITEM") }})
select *
from stg_lineas_pedido
