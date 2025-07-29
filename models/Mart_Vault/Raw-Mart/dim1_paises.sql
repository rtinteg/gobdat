with
    dim1_paises as (

        select
            rp.hub_pais_id as dim1_pais_id,
            rp.hub_region_id as dim1_region_id,
            rp.nombre_pais,
            rp.nombre_region,
            rp.fecha_carga as fecha_actual,
            rp.origen_pais as origen
        from
            {{ source("raw", "LNK_REGIONES_PAISES") }} rp,
            {{ source("raw", "SAT_PAISES") }} sp
        where
            rp.hub_pais_id = sp.hub_pais_id
            and sp.fecha_carga = (
                select max(fecha_carga)
                from {{ source("raw", "SAT_PAISES") }} sp2
                where rp.hub_pais_id = sp2.hub_pais_id
            )
    )
select *
from dim1_paises
