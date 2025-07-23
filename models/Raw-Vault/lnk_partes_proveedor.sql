with
    lnk_partes as (
        select
            p_partkey,
            p_name
        
        from {{ source("stg","STG_PARTES") }}
    )
select *
from lnk_partes,
    lnk_proveedor as (
        select
            s_suppkey,
            s_name
        from {{ source("stg","STG_PROVEEDORES") }}
    )
select *
from lnk_partes,
    lnk_partes_proveedor as (
        select 
            ps_partkey,
            ps_suppkey
        from {{ source("stg","STG_PARTES_PROVEEDOR") }}
    )
