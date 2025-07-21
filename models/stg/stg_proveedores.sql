-- {{ config(materialized="table") }}

with stg_proveedor as (select * from {{ source("stg", "SUPPLIER") }})
select *
from stg_proveedor
