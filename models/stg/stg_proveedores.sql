-- {{ config(materialized="table") }}

with stg_proveedor as (select * from {{ source("src", "SUPPLIER") }})
select *
from stg_proveedor
