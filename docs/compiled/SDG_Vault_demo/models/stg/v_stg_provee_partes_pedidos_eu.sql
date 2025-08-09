

with
    stg_provee_partes_pedidos_europa as (
        select *
        from SDGVAULTMART.DBT_SDGVAULT.STG_PROVEEDORES a
        join SDGVAULTMART.DBT_SDGVAULT.STG_PARTES_PROVEEDOR b on a.s_suppkey = b.ps_suppkey
        join SDGVAULTMART.DBT_SDGVAULT.STG_PARTES c on b.ps_partkey = c.p_partkey
        join
            SDGVAULTMART.DBT_SDGVAULT.v_stg_regiones_paises d
            on a.s_nationkey = d.n_nationkey
            and d.n_nationkey in (6, 19, 22, 23, 99, 7)
    )
select *
from stg_provee_partes_pedidos_europa