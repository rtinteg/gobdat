with
    sat_partes_proveedores as (
        select
            b.hub_parte_id,
            c.hub_proveedor_id,
            current_date as fecha_carga,
            md5(
                upper(trim(nvl(a.ps_availqty, '')))
                || upper(trim(nvl(a.ps_supplycost, '')))
                || upper(trim(nvl(a.ps_comment, '')))
                || upper(trim(nvl(a.ps_origen, '')))
            ) as foto_parte_pedido,
            a.ps_origen,
            a.ps_availqty as disponibilidad,
            a.ps_supplycost as coste_envio,
            a.ps_comment as comentario,
        from SDGVAULTMART.DBT_SDGVAULT.STG_PARTES_PROVEEDOR a
        join SDGVAULTMART.DBT_SDGVAULT.STG_PARTES a1 on a.ps_partkey = a1.p_partkey
        join SDGVAULTMART.DBT_SDGVAULT.STG_PROVEEDORES a2 on a.ps_suppkey = a2.s_suppkey
        join
            SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_PARTES b
            on a1.p_name = b.nombre_parte
            and a1.p_brand = b.nombre_marca
        join SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_PROVEEDORES c on a2.s_name = c.nombre_proveedor
    )
select *
from sat_partes_proveedores