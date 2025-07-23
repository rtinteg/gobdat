with
    stg_partes as (
        select
            md5(
                upper(trim(nvl(p_name, ''))) || upper(trim(nvl(p_brand, '')))
            ) as parte_id,
            p_name as nombre_parte,
            p_brand as nombre_marca
        from {{ source("stg", "STG_PARTES") }}
    ),
    stg_proveedor as (
        select
            md5(upper(trim(nvl(s_name, '')))) as proveedor_id,
            s_name as nombre_proveedor,
        from {{ source("stg", "STG_PROVEEDORES") }}
    )
select
    md5(
        upper(trim(nvl(p1.nombre_parte, '')))
        || upper(trim(nvl(p1.nombre_marca, '')))
        || upper(trim(nvl(p2.nombre_proveedor, '')))
    ) as lnk_partes_proveedor_id,
    p1.parte_id,
    p2.proveedor_id,
    p1.nombre_parte,
    p1.nombre_marca,
    p2.nombre_proveedor,
    current_date as fecha_carga,
    'Snowflake' as origen
from stg_partes p1, stg_proveedor p2
