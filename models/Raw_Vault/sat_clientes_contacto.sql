{{ config(materialized="incremental", unique_key="hub_cliente_id") }}

with
    sat_clientes_contacto as (
        {% if is_incremental() %}

            select
                b.hub_cliente_id,
                current_date as fecha_carga,
                md5(
                    upper(trim(nvl(c_address, '')))
                    || upper(trim(nvl(c_comment, '')))
                    || upper(trim(nvl(c_phone, '')))
                    || upper(trim(nvl(c_origen, '')))
                ) as foto_cliente,
                a.c_origen,
                a.c_address as direccion,
                a.c_comment as comentario,
                a.c_phone as telefono
            from {{ source("stg", "STG_CLIENTES") }} a
            join {{ source("raw", "HUB_CLIENTES") }} b on a.c_name = b.nombre_cliente
            where hub_cliente_id not in (select hub_cliente_id from {{ this }})
        {% else %}
            select
                b.hub_cliente_id,
                current_date as fecha_carga,
                md5(
                    upper(trim(nvl(c_address, '')))
                    || upper(trim(nvl(c_comment, '')))
                    || upper(trim(nvl(c_phone, '')))
                    || upper(trim(nvl(c_origen, '')))
                ) as foto_cliente,
                a.c_origen,
                a.c_address as direccion,
                a.c_comment as comentario,
                a.c_phone as telefono
            from {{ source("stg", "STG_CLIENTES") }} a
            join {{ source("raw", "HUB_CLIENTES") }} b on a.c_name = b.nombre_cliente
        {% endif %}
    )
select *
from sat_clientes_contacto
