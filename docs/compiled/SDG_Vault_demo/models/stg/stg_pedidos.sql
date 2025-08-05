

with
    source_data as (
        
            select
                o_orderkey,
                o_custkey,
                o_orderstatus,
                o_totalprice,
                o_orderdate,
                o_orderpriority,
                o_clerk,
                o_shippriority,
                o_comment,
                o_origen
            from SDGVAULTMART.DBT_SDGVAULT.PEDIDOS_ELT
            where o_orderkey not in (select o_orderkey from SDGVAULTMART.DBT_SDGVAULT.stg_pedidos)
        
    )
select *
from source_data