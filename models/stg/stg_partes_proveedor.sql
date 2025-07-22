-- {{ config(materialized="table") }}

with stg_partes_proveedor as (select * from {{ source("src", "PARTSUPP") }})
select *
from stg_partes_proveedor
