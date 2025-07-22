-- {{ config(materialized="table") }}

with stg_partes_region as (select * from {{ source("src", "REGION") }})
select *
from stg_partes_region
