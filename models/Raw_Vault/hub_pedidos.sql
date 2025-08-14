{{ config(materialized="incremental", unique_key=["clave_pedido", "empleado"]) }}

with
    hub_pedidos as (
        {% if is_incremental() %}
            select
                md5(
                    upper(trim(nvl(o_orderkey, ''))) || upper(trim(nvl(o_clerk, '')))
                ) as hub_pedido_id,
                o_orderkey as clave_pedido,
                o_clerk as empleado,
                load_date as fecha_carga,
                o_origen as origen
            from {{ source("stg", "STG_PEDIDOS") }}
            where
                (o_orderkey, o_clerk)
                not in (select clave_pedido, empleado from {{ this }})
        {% else %}
            select
                md5(
                    upper(trim(nvl(o_orderkey, ''))) || upper(trim(nvl(o_clerk, '')))
                ) as hub_pedido_id,
                o_orderkey as clave_pedido,
                o_clerk as empleado,
                load_date as fecha_carga,
                o_origen as origen
            from {{ source("stg", "STG_PEDIDOS") }}
        {% endif %}
    )
select *
from hub_pedidos
