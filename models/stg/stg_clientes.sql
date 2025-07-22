-- {{ config(materialized="table") }}

with stg_clientes as (select * from {{ source("src", "CUSTOMER") }})
select *
from stg_clientes
