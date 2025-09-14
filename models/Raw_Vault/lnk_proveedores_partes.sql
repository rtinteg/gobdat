with
    hub_proveedores as (
        select hub_proveedor_id, nombre_proveedor,
        from {{ source("raw", "HUB_PROVEEDORES") }}
    ),
    hub_partes as (
        select hub_parte_id, nombre_parte, nombre_marca
        from {{ source("raw", "HUB_PARTES") }}
    ),
    stg_proveedores_partes as (
        select a.s_name, c.p_name, c.p_brand, a.s_origen, c.p_origen
        from {{ source("stg", "STG_PROVEEDORES") }} a
        join {{ source("stg", "STG_PARTES_PROVEEDOR") }} b on a.s_suppkey = b.ps_suppkey
        join {{ source("stg", "STG_PARTES") }} c on b.ps_partkey = c.p_partkey
    )
select
    md5(
        upper(trim(nvl(p1.nombre_proveedor, '')))
        || upper(trim(nvl(p2.nombre_parte, '')))
        || upper(trim(nvl(p2.nombre_marca, '')))
    ) as lnk_proveedor_parte_id,
    p1.hub_proveedor_id,
    p2.hub_parte_id,
    p1.nombre_proveedor,
    p2.nombre_parte,
    p2.nombre_marca,
    current_date as fecha_carga,
    p3.s_origen as origen_proveedor,
    p3.p_origen as origen_parte
from hub_proveedores p1, hub_partes p2, stg_proveedores_partes p3
where
    p1.nombre_proveedor = p3.s_name
    and (p2.nombre_parte = p3.p_name and p2.nombre_marca = p3.p_brand)
