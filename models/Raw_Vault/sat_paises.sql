{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["hub_pais_id", "fecha_carga"],
    )
}}

with
    src as (
        select
            b.hub_pais_id,
            a.load_date as fecha_carga,  
            md5(upper(trim(coalesce(a.n_comment, '')))) as foto_pais,
            a.n_origen as n_origen,
            a.n_comment as comentario
        from {{ source("stg", "STG_PAISES") }} a
        join {{ source("raw", "HUB_PAISES") }} b on a.n_name = b.nombre_pais
    ),

    sat_paises as (
        select s.*
        from src s
        {% if is_incremental() %}
            left join
                {{ this }} t
                on t.hub_pais_id = s.hub_pais_id
                and t.fecha_carga = s.fecha_carga
                and t.foto_pais = s.foto_pais
            where t.hub_pais_id is null
        {% endif %}
    )

select *
from sat_paises
