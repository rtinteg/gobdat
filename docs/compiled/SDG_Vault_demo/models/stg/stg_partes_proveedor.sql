with stg_partes_proveedor as (select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PARTSUPP)
select
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    ps_comment,
    'Snowflake' as ps_origen
from stg_partes_proveedor