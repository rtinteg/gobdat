

with
    stg_clientes_pedidos_europa as (
        select *
        from SDGVAULTMART.DBT_SDGVAULT.STG_CLIENTES a
        join SDGVAULTMART.DBT_SDGVAULT.STG_PEDIDOS b on a.c_custkey = b.o_custkey
        join
            SDGVAULTMART.DBT_SDGVAULT.v_stg_regiones_paises c
            on a.c_nationkey = c.n_nationkey
            and c.n_nationkey in (6, 19, 22, 23, 99, 7)
    )
select *
from stg_clientes_pedidos_europa