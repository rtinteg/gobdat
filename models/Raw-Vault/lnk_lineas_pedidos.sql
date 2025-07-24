with
    stg_partes as (
        select
            md5(
                upper(trim(nvl(p_name, ''))) || upper(trim(nvl(p_brand, '')))
            ) as parte_id,
            p_name as nombre_parte,
            p_brand as nombre_marca
        from {{ source("stg", "STG_PARTES") }}
        where p_type = 'ECONOMY ANODIZED STEEL'
      -- para reducir tiempos y limitar el volumen se filtra por tipos
    ),
    stg_proveedor as (
        select
            md5(upper(trim(nvl(s_name, '')))) as proveedor_id,
            s_name as nombre_proveedor
        from {{ source("stg", "STG_PROVEEDORES") }}
        where s_nationkey = 22  -- para reducir tiempos y limitar el volumen se filtra para EEUU, UK, RUSIA y ALEMANIA
    ),
    stg_pedidos as (
        select
            md5(
                upper(trim(nvl(o_orderkey, ''))) || upper(trim(nvl(o_clerk, '')))
            ) as pedido_id,
            o_orderkey as clave_pedido,
            o_clerk as empleado
        from {{ source("stg", "STG_PEDIDOS") }}
        where o_clerk = 'Clerk#000000542' -- para reducir tiempos y limitar el volumen se filtra por empleado
    )
select
    md5(
        upper(trim(nvl(p1.nombre_parte, '')))
        || upper(trim(nvl(p1.nombre_marca, '')))
        || upper(
            trim(nvl(p2.nombre_proveedor, ''))
            || upper(trim(nvl(p3.clave_pedido, '')))
            || upper(trim(nvl(p3.empleado, '')))
        )
    ) as lnk_partes_proveedor_id,
    p1.parte_id,
    p2.proveedor_id,
    p3.pedido_id,
    p1.nombre_parte,
    p1.nombre_marca,
    p2.nombre_proveedor,
    p3.clave_pedido,
    p3.empleado,
    current_date as fecha_carga,
    'Snowflake' as origen
from stg_partes p1, stg_proveedor p2, stg_pedidos p3
