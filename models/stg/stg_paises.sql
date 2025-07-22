-- {{ config(materialized="table") }}

with stg_paises as (select * from {{ source("src", "NATION") }})
select *
from stg_paises
