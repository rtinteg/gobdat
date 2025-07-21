-- {{ config(materialized="table") }}

with stg_partes as (select * from {{ source("stg", "PART") }})
select *
from stg_partes
