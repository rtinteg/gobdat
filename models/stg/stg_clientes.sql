{{ config(materialized="incremental", unique_key="c_custkey") }}

-- Selección condicional de la fuente de datos
with
    source_data as (
        {% if is_incremental() %}
            select
                c_acctbal,
                c_address,
                c_comment,
                c_custkey,
                c_mktsegment,
                c_name,
                c_nationkey,
                c_phone,
                c_origen
            from {{ source("stg", "CLIENTES_ELT") }}
            where c_custkey not in (select c_custkey from {{ this }})
        {% else %}
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
            from {{ source("src", "CUSTOMER") }}
        {% endif %}
    )
select *
from source_data
