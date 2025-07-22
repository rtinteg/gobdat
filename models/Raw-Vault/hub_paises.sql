with
    hub_paises as (
        select
            md5(upper(trim(nvl(m_name, '')))) as hub_pais_id,
            n_name as nombre_pais,
            current_date as fecha_carga,
            'Snowflake' as origen
        -- from {{ ref("stg_clientes")}}
        from {{ source("stg","STG_PAISES")}}
    )
select *
from hub_paises