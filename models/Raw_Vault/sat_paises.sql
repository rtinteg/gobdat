{{ config(materialized="incremental", unique_key="hub_pais_id") }}

with
    sat_paises as (
        {% if is_incremental() %}
            select
                hub_pais_id,
                current_date as fecha_carga,
                md5(
                    upper(trim(nvl(n_comment, ''))) || upper(trim(nvl(n_origen, '')))
                ) as foto_pais,
                n_origen,
                n_comment as comentario,
            from {{ source("stg", "STG_PAISES") }} a
            join {{ source("raw", "HUB_PAISES") }} b on a.n_name = b.nombre_pais 
            where hub_pais_id not in (select hub_pais_id from {{ this }})
        {% else %}
            select
                b.hub_pais_id,
                current_date as fecha_carga,
                md5(
                    upper(trim(nvl(n_comment, ''))) || upper(trim(nvl(n_origen, '')))
                ) as foto_pais,
                a.n_origen,
                a.n_comment as comentario,
            from {{ source("stg", "STG_PAISES") }} a
            join {{ source("raw", "HUB_PAISES") }} b on a.n_name = b.nombre_pais
        {% endif %}
    )
select *
from sat_paises
