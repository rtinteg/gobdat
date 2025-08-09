

with
    sat_clientes_contacto as (
        select
            b.hub_cliente_id,
            b.fecha_carga,
            md5(
                upper(trim(nvl(c_address, '')))
                || upper(trim(nvl(c_comment, '')))
                || upper(trim(nvl(c_phone, '')))
            ) as foto_cliente,
            a.c_origen,
            a.c_address as direccion,
            a.c_comment as comentario,
            a.c_phone as telefono
        from SDGVAULTMART.DBT_SDGVAULT.STG_CLIENTES a
        join SDGVAULTMART.DBT_SDGVAULT_BRONZE.HUB_CLIENTES b on a.c_name = b.nombre_cliente
        
            where
                not exists (
                    select 1
                    from SDGVAULTMART.DBT_SDGVAULT_BRONZE.sat_clientes_contacto s
                    where
                        s.hub_cliente_id = b.hub_cliente_id
                        and s.foto_cliente = md5(
                            upper(trim(coalesce(a.c_address, '')))
                            || upper(trim(coalesce(a.c_comment, '')))
                            || upper(trim(coalesce(a.c_phone, '')))
                        )
                )
        
    )
select *
from sat_clientes_contacto