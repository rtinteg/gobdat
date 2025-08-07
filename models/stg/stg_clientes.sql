{{ config(materialized="incremental", unique_key=["c_custkey", "load_date"]) }}

-- Fuente condicional de datos según tipo de carga
with
    source_data as (

        {% if is_incremental() %}

            -- Incremental: datos nuevos desde CSV
            select
                c_acctbal,
                c_address,
                c_comment,
                c_custkey,
                c_mktsegment,
                c_name,
                c_nationkey,
                c_phone,
                'CSV' as c_origen,
                current_date - 2 as load_date
            from {{ source("stg", "CLIENTES_ELT") }}

        {% else %}

            -- Primera carga: desde tabla fuente intocable (sin origen ni fecha)
            select
                c_acctbal,
                c_address,
                c_comment,
                c_custkey,
                c_mktsegment,
                c_name,
                c_nationkey,
                c_phone,
                'Snowflake' as c_origen,
                current_date - 15 as load_date
            from {{ source("src", "CUSTOMER") }}

        {% endif %}

    )

select *
from source_data
