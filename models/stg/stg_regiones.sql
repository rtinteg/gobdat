with stg_partes_region as (select * from {{ source("src", "REGION") }})
select r_regionkey, r_name, r_comment, 'Snowflake' as r_origen
from stg_partes_region
