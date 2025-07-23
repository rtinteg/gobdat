with stg_clientes as (select * from {{ source("src", "CUSTOMER") }})
select
    c_acctbal,
    c_address,
    c_comment,
    c_custkey,
    c_mktsegment,
    c_name,
    c_nationkey,
    c_phone,
    'Snowflake' as c_origen
from stg_clientes
