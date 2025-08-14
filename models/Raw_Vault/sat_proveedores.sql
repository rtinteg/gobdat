with
    sat_proveedores as (
        select
            b.hub_proveedor_id,
            current_date as fecha_carga,
            md5(
                upper(trim(nvl(s_acctbal, '')))
                || upper(trim(nvl(s_address, '')))
                || upper(trim(nvl(s_comment, '')))
                || upper(trim(nvl(s_phone, '')))
                || upper(trim(nvl(s_origen, '')))
            ) as foto_proveedor,
            a.s_origen,
            a.s_acctbal as cuenta_balance,
            a.s_address as direccion,
            a.s_comment as comentario,
            a.s_phone as telefono
        from {{ source("stg", "STG_PROVEEDORES") }} a
        join {{ source("raw", "HUB_PROVEEDORES") }} b on a.s_name = b.nombre_proveedor
    )
select *
from sat_proveedores
