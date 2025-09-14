

with
    hub_paises as (
        select hub_pais_id, nombre_pais from SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_PAISES
    ),
    hub_proveedores as (
        select hub_proveedor_id, nombre_proveedor, fecha_carga
        from SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_PROVEEDORES
    ),
    stg_paises_proveedores as (
        select a.n_name, b.s_name, a.n_origen, b.s_origen--, b.load_date
        from SDGVAULTMART.DBT_SDGVAULT.STG_PAISES a
        join SDGVAULTMART.DBT_SDGVAULT.STG_PROVEEDORES b on a.n_nationkey = b.s_nationkey
    ),
    combinaciones as (
        select
            md5(
                upper(trim(coalesce(p1.nombre_pais, '')))
                || upper(trim(coalesce(p2.nombre_proveedor, '')))
            ) as lnk_pais_proveedor_id,
            p1.hub_pais_id,
            p2.hub_proveedor_id,
            p1.nombre_pais,
            p2.nombre_proveedor,
            p2.fecha_carga,
            p3.n_origen as origen_pais,
            p3.s_origen as origen_proveedor
        from hub_paises p1
        join stg_paises_proveedores p3 on p1.nombre_pais = p3.n_name
        join hub_proveedores p2 on p2.nombre_proveedor = p3.s_name
    )

select *
from combinaciones


    -- Evita insertar vínculos ya existentes
    where lnk_pais_proveedor_id not in (select lnk_pais_proveedor_id from SDGVAULTMART.DBT_SDGVAULT_BRONZE.lnk_paises_proveedores)
