with
    hub_paises as (
        select hub_pais_id, nombre_pais, from {{ source("raw", "HUB_PAISES") }}
    ),
    hub_proveedores as (
        select hub_proveedor_id, nombre_proveedor
        from {{ source("raw", "HUB_PROVEEDORES") }}
    ),
    stg_paises_proveedores as (
        select a.n_name, b.s_name, a.n_origen, b.s_origen
        from {{ source("stg", "STG_PAISES") }} a
        join {{ source("stg", "STG_PROVEEDORES") }} b
        where a.n_nationkey = b.s_nationkey
    )
select
    md5(
        upper(trim(nvl(p1.nombre_pais, '')))
        || upper(trim(nvl(p2.nombre_proveedor, '')))
    ) as lnk_pais_proveedor_id,
    p1.hub_pais_id,
    p2.hub_proveedor_id,
    p1.nombre_pais,
    p2.nombre_proveedor,
    current_date as fecha_carga,
    n_origen as origen_pais,
    s_origen as origen_proveedor
from hub_paises p1, hub_proveedores p2, stg_paises_proveedores p3
where p1.nombre_pais = p3.n_name and p2.nombre_proveedor = p3.s_name
