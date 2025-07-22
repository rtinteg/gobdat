with
    hub_proveedores as (
        select
            md5(upper(trim(nvl(s_name, '')))) as hub_proveedor_id,
            s_name as nombre_proveedor,
            current_date as fecha_carga,
            'Snowflake' as origen
        
        from {{ source("stg","STG_PROVEEDORES") }}
    )
select *
from hub_proveedores