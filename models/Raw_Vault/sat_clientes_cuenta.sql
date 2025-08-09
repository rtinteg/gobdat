{{ config(materialized="incremental", unique_key=["hub_cliente_id", "fecha_carga"]) }}

with
    sat_clientes_cuenta as (
        select
            b.hub_cliente_id,
            b.fecha_carga,
            md5(
                upper(trim(nvl(c_acctbal, '')))
                || upper(trim(nvl(c_comment, '')))
                || upper(trim(nvl(c_mktsegment, '')))
            ) as foto_cliente,
            a.c_origen,
            a.c_acctbal as cuenta_balance,
            a.c_comment as comentario,
            a.c_mktsegment as segmento_marketing
        from {{ source("stg", "STG_CLIENTES") }} a
        join {{ source("raw", "HUB_CLIENTES") }} b on a.c_name = b.nombre_cliente
        {% if is_incremental() %}
            where
                not exists (
                    select 1
                    from {{ this }} s
                    where
                        s.hub_cliente_id = b.hub_cliente_id
                        and s.foto_cliente = md5(
                            upper(trim(nvl(c_acctbal, '')))
                            || upper(trim(nvl(c_comment, '')))
                            || upper(trim(nvl(c_mktsegment, '')))
                        )
                        and s.fecha_carga = b.fecha_carga
                )
        {% endif %}
    )
select *
from sat_clientes_cuenta
