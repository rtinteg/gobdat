{{ config(materialized="incremental", unique_key=["n_nationkey", "load_date"]) }}

-- Fuente condicional de datos según tipo de carga
with
    source_data as (
        {% if is_incremental() %}

            -- Incremental: datos nuevos desde CSV
            select
                n_nationkey,
                n_name,
                n_regionkey,
                n_comment,
                n_origen,
                current_date as load_date
            from {{ source("stg", "PAISES_ELT") }}

        {% else %}

            -- Primera carga: desde tabla fuente intocable (sin origen ni fecha)
            select
                n_nationkey,
                n_name,
                n_regionkey,
                n_comment,
                'Snowflake' as n_origen,
                current_date - 15 as load_date
            from {{ source("src", "NATION") }}
        {% endif %}
    )

select *
from source_data
