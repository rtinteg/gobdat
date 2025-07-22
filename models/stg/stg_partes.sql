-- {{ config(materialized="table") }}

with stg_partes as (select * from {{ source("src", "PART") }})
select *
from stg_partes
