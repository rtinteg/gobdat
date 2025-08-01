{{ config(materialized="view") }}

with
    stg_provee_partes_pedidos_europa as (
        select *
        from {{ source("stg", "STG_PROVEEDORES") }} a
        join {{ source("stg", "STG_PARTES_PROVEEDOR") }} b on a.s_suppkey = b.ps_suppkey
        join {{ source("stg", "STG_PARTES") }} c on b.ps_partkey = c.p_partkey
        join
            {{ ref("v_stg_regiones_paises") }} d
            on a.s_nationkey = d.n_regionkey
            and d.n_nationkey in (6, 19, 22, 23, 99, 7)
    )
select *
from stg_provee_partes_pedidos_europa
