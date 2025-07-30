{{ config(materialized="incremental", unique_key="n_nationkey") }}

-- Selección condicional de la fuente de datos
with
    source_data as (

        {% if is_incremental() %}
            select n_nationkey, n_name, n_regionkey, n_comment, n_origen
            from {{ source("stg", "PAISES_ELT") }}
            where n_nationkey not in (select n_nationkey from {{ this }})
        {% else %}
            select n_nationkey, n_name, n_regionkey, n_comment, 'Snowflake' as n_origen
            from {{ source("src", "NATION") }}
        {% endif %}

    )
select *
from source_data
