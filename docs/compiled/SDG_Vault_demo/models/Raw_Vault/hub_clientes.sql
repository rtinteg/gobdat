

with
    hub_paises as (
        
            select
                md5(upper(trim(nvl(c_name, '')))) as hub_cliente_id,
                c_name as nombre_cliente,
                load_date as fecha_carga,
                c_origen as origen
            from SDGVAULTMART.DBT_SDGVAULT.STG_CLIENTES
            where c_name not in (select nombre_cliente from SDGVAULTMART.DBT_SDGVAULT_BRONZE.hub_clientes)
        
    )
select *
from hub_paises