{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["hub_pedido_id", "fecha_carga"],
    )
}}

with
    src as (
        select
            b.hub_pedido_id,
            a.fecha_carga as fecha_carga,  
            md5(
                upper(trim(coalesce(a.o_orderstatus, '')))
                || upper(trim(coalesce(cast(a.o_totalprice as string), '')))
                || upper(trim(coalesce(cast(a.o_orderdate as string), '')))
                || upper(trim(coalesce(a.o_orderpriority, '')))
                || upper(trim(coalesce(cast(a.o_shippriority as string), '')))
                || upper(trim(coalesce(a.o_comment, '')))
            ) as foto_pedido,
            a.o_origen,
            a.o_orderstatus as estado_pedido,
            a.o_totalprice as precio_total,
            a.o_orderdate as fecha_pedido,
            a.o_orderpriority as prioridad_ped,
            a.o_shippriority as prioridad_env,
            a.o_comment as comentario
        from {{ source("stg", "STG_PEDIDOS") }} a
        join
            {{ source("raw", "HUB_PEDIDOS") }} b
            on a.o_orderkey = b.clave_pedido
            and a.o_clerk = b.empleado
    ),

    to_insert as (
        select s.*
        from src s
        {% if is_incremental() %}
            left join
                {{ this }} t
                on t.hub_pedido_id = s.hub_pedido_id
                and t.fecha_carga = s.fecha_carga
                and t.foto_pedido = s.foto_pedido
            where t.hub_pedido_id is null
        {% endif %}
    )

select *
from to_insert
