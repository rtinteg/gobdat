select
    hp.hub_pais_id as dim1_pais_id,
    hp.nombre_pais,
    sp.comentario,
    sp.fecha_carga as fecha_actual,
    sp.n_origen as origen
from {{ source("raw", "HUB_PAISES") }} hp, {{ source("raw", "SAT_PAISES") }} sp
where
    hp.hub_pais_id = sp.hub_pais_id
    and sp.fecha_carga = (
        select max(fecha_carga)
        from {{ source("raw", "SAT_PAISES") }} sp2
        where hub_pais_id = sp2.hub_pais_id
    )
