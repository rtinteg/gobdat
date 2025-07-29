with
    sat_clientes_contacto as (
        select
            b.hub_cliente_id,
            current_date as fecha_carga,
            md5(
                upper(trim(nvl(c_address, '')))
                || upper(trim(nvl(c_comment, '')))
                || upper(trim(nvl(c_phone, '')))
                || upper(trim(nvl(c_origen, '')))
            ) as foto_cliente,
            a.c_origen,
            a.c_address as direccion,
            a.c_comment as comentario,
            a.c_phone as telefono
        from {{ source("stg", "STG_CLIENTES") }} a
        join {{ source("raw", "HUB_CLIENTES") }} b on a.c_name = b.nombre_cliente
    )
select *
from sat_clientes_contacto
