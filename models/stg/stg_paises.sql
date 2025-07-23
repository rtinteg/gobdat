with stg_paises as (select * from {{ source("src", "NATION") }})
select n_nationkey, n_name, n_regionkey, n_comment, 'Snowflake' as n_origen
from stg_paises
