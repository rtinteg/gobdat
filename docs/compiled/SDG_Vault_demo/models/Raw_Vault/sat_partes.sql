with
    sat_partes as (
        select
            b.hub_parte_id,
            current_date as fecha_carga,
            md5(
                upper(trim(nvl(a.p_mfgr, '')))
                || upper(trim(nvl(a.p_type, '')))
                || upper(trim(nvl(a.p_size, '')))
                || upper(trim(nvl(a.p_container, '')))
                || upper(trim(nvl(a.p_retailprice, '')))
                || upper(trim(nvl(a.p_comment, '')))
                || upper(trim(nvl(a.p_origen, '')))
            ) as foto_parte,
            a.p_origen,
            a.p_mfgr as fabricante,
            a.p_type as tipo,
            a.p_size as medida,
            a.p_container as contenedor,
            a.p_retailprice as precio_venta,
            a.p_comment as comentario,
        from SDGVAULTMART.DBT_SDGVAULT.STG_PARTES a
        join
            SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_PARTES b
            on a.p_name = b.nombre_parte
            and a.p_brand = b.nombre_marca
    )
select *
from sat_partes