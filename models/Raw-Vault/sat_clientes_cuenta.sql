with
    sat_clientes as (
        select
            b.hub_cliente_id,
            current_date as fecha_carga,
            md5(
                upper(trim(nvl(c_acctbal, '')))
                || upper(trim(nvl(c_comment, '')))
                || upper(trim(nvl(c_mktsegment, '')))
                || upper(trim(nvl(c_origen, '')))
            ) as foto_cliente,
            a.c_origen,
            a.c_acctbal as cuenta_balance,
            a.c_comment as comentario,
            a.c_mktsegment as segmento_marketing
        from {{ source("stg", "STG_CLIENTES") }} a
        join {{ source("raw", "HUB_CLIENTES") }} b on a.c_name = b.nombre_cliente
    )
select *
from sat_clientes
