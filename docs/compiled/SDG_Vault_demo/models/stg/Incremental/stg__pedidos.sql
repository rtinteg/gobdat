

with
    csv_nuevos as (
        select
            c.o_orderkey,
            c.o_custkey,
            c.o_orderstatus,
            c.o_totalprice,
            c.o_orderdate,
            c.o_orderpriority,
            c.o_clerk,
            c.o_shippriority,
            c.o_comment,
            c.o_origen
        from SDGVAULTMART.DBT_SDGVAULT.PEDIDOS_ELT c
    ),

     filtrados as (select * from csv_nuevos)
    

select *
from filtrados