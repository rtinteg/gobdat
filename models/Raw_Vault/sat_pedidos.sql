{{ config(materialized="incremental", unique_key=["hub_pedido_id", "fecha_carga"]) }}

with
    sat_pedidos as (
        select
            b.hub_pedido_id,
            b.fecha_carga,
            md5(
                upper(trim(coalesce(a.o_orderstatus, '')))
                || upper(trim(coalesce(cast(a.o_totalprice as string), '')))
                || upper(trim(coalesce(cast(a.o_orderdate as string), '')))
                || upper(trim(coalesce(a.o_orderpriority, '')))
                || upper(trim(coalesce(a.o_shippriority, '')))
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

        {% if is_incremental() %}
            -- Solo insertar si la versión (foto) no existe ya
            where
                not exists (
                    select 1
                    from {{ this }} s
                    where
                        s.hub_pedido_id = b.hub_pedido_id
                        and s.foto_pedido = md5(
                            upper(trim(coalesce(a.o_orderstatus, '')))
                            || upper(trim(coalesce(cast(a.o_totalprice as string), '')))
                            || upper(trim(coalesce(cast(a.o_orderdate as string), '')))
                            || upper(trim(coalesce(a.o_orderpriority, '')))
                            || upper(trim(coalesce(a.o_shippriority, '')))
                            || upper(trim(coalesce(a.o_comment, '')))
                        )
                )
        {% endif %}
    )
select *
from sat_pedidos
