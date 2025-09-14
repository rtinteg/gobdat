{{
    config(
        materialized="incremental",
        unique_key=["hub_cliente_id", "fecha_carga"],
        incremental_strategy="merge",
    )
}}

with
    src as (
        select
            b.hub_cliente_id,
            a.load_date as fecha_carga,  
            md5(
                upper(trim(coalesce(a.c_address::varchar, '')))
                || upper(trim(coalesce(a.c_comment, '')))
                || upper(trim(coalesce(a.c_phone::varchar, '')))
            ) as foto_cliente,
            a.c_origen,
            a.c_address as direccion,
            a.c_comment as comentario,
            a.c_phone as telefono
        from {{ source("stg", "STG_CLIENTES") }} a
        join {{ source("raw", "HUB_CLIENTES") }} b on a.c_name = b.nombre_cliente
    ),

    sat_clientes_contacto as (
        select s.*
        from src s
        {% if is_incremental() %}
            left join
                {{ this }} t
                on t.hub_cliente_id = s.hub_cliente_id
                and t.fecha_carga = s.fecha_carga
                and t.foto_cliente = s.foto_cliente
            where t.hub_cliente_id is null
        {% endif %}
    )

select *
from sat_clientes_contacto
