{{ config(materialized="incremental", unique_key="nombre_pais") }}

with
    hub_paises as (
        {% if is_incremental() %}
            select
                md5(upper(trim(nvl(n_name, '')))) as hub_pais_id,
                n_name as nombre_pais,
                current_date as fecha_carga,
                n_origen as origen
            from {{ source("stg", "STG_PAISES") }}
            where n_name not in (select nombre_pais from {{ this }})
        {% else %}
            select
                md5(upper(trim(nvl(n_name, '')))) as hub_pais_id,
                n_name as nombre_pais,
                current_date as fecha_carga,
                n_origen as origen
            from {{ source("stg", "STG_PAISES") }}
        {% endif %}
    )
select *
from hub_paises
