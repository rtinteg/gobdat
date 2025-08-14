with stg_partes_region as (select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION)
select r_regionkey, r_name, r_comment, 'Snowflake' as r_origen
from stg_partes_region