

with
    source_data as (

        

            -- Incremental: carga desde CSV
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
                'CSV' as o_origen,
                current_date -2 as load_date
            from SDGVAULTMART.DBT_SDGVAULT.PEDIDOS_ELT

        

    )

select *
from source_data