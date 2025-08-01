{{ config(materialized="incremental", unique_key="nombre_cliente") }}

with
    hub_paises as (
        {% if is_incremental() %}
            select
                md5(upper(trim(nvl(c_name, '')))) as hub_cliente_id,
                c_name as nombre_cliente,
                current_date as fecha_carga,
                c_origen as origen
            from {{ source("stg", "STG_CLIENTES") }}
            where c_name not in (select nombre_cliente from {{ this }})
        {% else %}
            select
                md5(upper(trim(nvl(c_name, '')))) as hub_cliente_id,
                c_name as nombre_cliente,
                current_date as fecha_carga,
                c_origen as origen
            from {{ source("stg", "STG_CLIENTES") }}
        {% endif %}
    )
select *
from hub_paises
