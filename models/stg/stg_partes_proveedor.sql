with stg_partes_proveedor as (select * from {{ source("src", "PARTSUPP") }})
select
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    ps_comment,
    'Snowflake' as ps_origen
from stg_partes_proveedor
