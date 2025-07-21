-- {{ config(materialized="table") }}

with stg_partes_region as (select * from {{ source("stg", "REGION") }})
select *
from stg_partes_region
