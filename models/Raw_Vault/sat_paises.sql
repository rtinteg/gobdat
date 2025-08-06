{{ config(materialized="incremental", unique_key=["hub_pais_id", "fecha_carga"]) }}

with
    sat_paises as (
        select
            b.hub_pais_id,
            b.fecha_carga,
            md5(upper(trim(nvl(n_comment, '')))) as foto_pais,
            n_origen,
            n_comment as comentario
        from {{ source("stg", "STG_PAISES") }} a
        join {{ source("raw", "HUB_PAISES") }} b on a.n_name = b.nombre_pais

        {% if is_incremental() %}
            where
                not exists (
                    select 1
                    from {{ this }} s
                    where
                        s.hub_pais_id = b.hub_pais_id
                        and s.foto_pais = md5(upper(trim(nvl(n_comment, ''))))
                )
        {% endif %}
    )
select *
from sat_paises
