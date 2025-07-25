with
    sat_pedidos as (
        select
            b.hub_pedido_id,
            current_date as fecha_carga,
            md5(
                upper(trim(nvl(a.o_orderstatus, '')))
                || upper(trim(nvl(a.o_totalprice, '')))
                || upper(trim(nvl(a.o_orderdate, '')))
                || upper(trim(nvl(a.o_orderpriority, '')))
                || upper(trim(nvl(a.o_shippriority, '')))
                || upper(trim(nvl(a.o_comment, '')))
                || upper(trim(nvl(a.o_origen, '')))
            ) as foto_pedido,
            a.o_origen,
            a.o_orderstatus as estado_pedido,
            a.o_totalprice as precio_total,
            a.o_orderdate as fecha_pedido,
            a.o_orderpriority as prioridad_ped,
            a.o_shippriority as prioridad_env,
            a.o_comment as comentario,
        from {{ source("stg", "STG_PEDIDOS") }} a
        join
            {{ source("raw", "HUB_PEDIDOS") }} b
            on a.o_orderkey = b.clave_pedido
            and a.o_clerk = b.empleado
    )
select *
from sat_pedidos
