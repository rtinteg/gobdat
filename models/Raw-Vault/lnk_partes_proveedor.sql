with
    hub_partes as (
        select
            hub_parte_id,
            nombre_parte        
        from {{ source("raw","HUB_PARTES") }}
    )
,
    hub_proveedor as (
        select
            hub_proveedor_id,
            nombre_proveedor
        from {{ source("raw","HUB_PROVEEDORES") }}
    )

select md5(upper(trim(nvl(p1.nombre_parte, ''))) || upper(trim(nvl(p2.nombre_proveedor, '')))) as lnk_partes_proveedor_id,
    p1.hub_parte_id,
    p2.hub_proveedor_id,
    p1.nombre_parte,
    p2.nombre_proveedor,
    current_date as fecha_carga,
    'Snowflake' as origen      
from hub_partes p1,
     hub_proveedor p2


