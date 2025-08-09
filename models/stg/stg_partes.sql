with stg_partes as (select * from {{ source("src", "PART") }})
select
    p_partkey,
    p_name,
    p_mfgr,
    p_brand,
    p_type,
    p_size,
    p_container,
    p_retailprice,
    p_comment,
    'Snowflake' as p_origen
from stg_partes
