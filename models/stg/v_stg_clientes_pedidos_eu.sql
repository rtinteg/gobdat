{{ config(materialized="view") }}

with
    stg_clientes_pedidos_europa as (
        select *
        from {{ source("stg", "STG_CLIENTES") }} a
        join {{ source("stg", "STG_PEDIDOS") }} b on a.c_custkey = b.o_custkey
        join
            {{ ref("v_stg_regiones_paises") }} c
            on a.c_nationkey = c.n_nationkey
            and c.n_nationkey in (6, 19, 22, 23, 99, 7)
    )
select *
from stg_clientes_pedidos_europa
