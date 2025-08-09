with stg_proveedor as (select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER)
select
    s_suppkey,
    s_name,
    s_address,
    s_nationkey,
    s_phone,
    s_acctbal,
    s_comment,
    'Snowflake' as s_origen
from stg_proveedor