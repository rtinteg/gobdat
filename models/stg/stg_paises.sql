-- {{ config(materialized="table") }}

with stg_paises as (select * from {{ source("stg", "NATION") }})
select *
from stg_paises
