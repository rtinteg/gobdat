with
    sat_lineas_pedidos as (
        select
            b.hub_lineas_pedido_id,
            current_date as fecha_carga,
            md5(
                upper(trim(nvl(a.l_quantity, '')))
                || upper(trim(nvl(a.l_extendedprice, '')))
                || upper(trim(nvl(a.l_discount, '')))
                || upper(trim(nvl(a.l_tax, '')))
                || upper(trim(nvl(a.l_returnflag, '')))
                || upper(trim(nvl(a.l_linestatus, '')))
                || upper(trim(nvl(a.l_shipdate, '')))
                || upper(trim(nvl(a.l_commitdate, '')))
                || upper(trim(nvl(a.l_receiptdate, '')))
                || upper(trim(nvl(a.l_shipinstruct, '')))
                || upper(trim(nvl(a.l_shipmode, '')))
                || upper(trim(nvl(a.l_comment, '')))
                || upper(trim(nvl(a.l_origen, '')))
            ) as foto_linea_pedido,
            a.l_origen,
            a.l_quantity,
            a.l_extendedprice,
            a.l_discount,
            a.l_tax,
            a.l_returnflag,
            a.l_linestatus,
            a.l_shipdate,
            a.l_commitdate,
            a.l_receiptdate,
            a.l_shipinstruct,
            a.l_shipmode,
            a.l_comment
        from {{ source("stg", "STG_LINEAS_PEDIDO") }} a
        join
            {{ source("raw", "HUB_LINEAS_PEDIDOS") }} b
            on a.l_orderkey = b.clave_pedido
            and a.l_linenumber = b.linea_pedido
    )
select *
from sat_lineas_pedidos
