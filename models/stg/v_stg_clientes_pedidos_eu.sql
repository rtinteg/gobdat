{{ config(materialized="view") }}

with
    stg_clientes_pedidos_europa as (
        select
            c_acctbal,
            c_address,
            c_comment,
            c_custkey,
            c_mktsegment,
            c_name,
            c_nationkey,
            c_phone,
            c_origen,
            a.load_date as fecha_cliente,
            o_orderkey,
            o_custkey,
            o_orderstatus,
            o_totalprice,
            o_orderdate,
            o_orderpriority,
            o_clerk,
            o_shippriority,
            o_comment,
            o_origen,
            b.load_date as fecha_pedido,
            r_regionkey,
            r_name,
            r_comment,
            r_origen,
            n_nationkey,
            n_name,
            n_regionkey,
            n_comment,
            n_origen,
            c.load_date as fecha_pais
        from sdgvaultmart.dbt_sdgvault.stg_clientes a
        join sdgvaultmart.dbt_sdgvault.stg_pedidos b on a.c_custkey = b.o_custkey
        join
            sdgvaultmart.dbt_sdgvault.v_stg_regiones_paises c
            on a.c_nationkey = c.n_nationkey
            and c.n_regionkey = 3
    )
select *
from stg_clientes_pedidos_europa
