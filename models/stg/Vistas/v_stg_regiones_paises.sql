{{ config(materialized="view") }}

select a.*, b.*
from {{ source("stg", "STG_REGIONES") }} a
join {{ source("stg", "STG_PAISES") }} b on a.r_regionkey = b.n_regionkey
