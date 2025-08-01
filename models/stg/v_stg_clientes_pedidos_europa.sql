{{ config(materialized="view") }}

with
    stg_clientes_pedidos_europa as (
        select *
        from {{ source("stg", "STG_CLIENTES") }} a
        join {{ source("stg", "STG_PEDIDOS") }} b on a.c_custkey = b.o_custkey
        join {{ ref("v_stg_regiones_paises") }} c on a.c_nationkey = c.n_regionkey
    )
select *
from stg_clientes_pedidos_europa
