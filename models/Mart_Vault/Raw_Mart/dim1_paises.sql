{{
    config(
        materialized="incremental",
        unique_key="dim1_pais_id",
        merge_update_columns=[
            "dim1_region_id",
            "nombre_pais",
            "nombre_region",
            "fecha_actual",
        ],
    )
}}

with
    dim1_paises as (
        select
            rp.hub_pais_id as dim1_pais_id,
            rp.hub_region_id as dim1_region_id,
            rp.nombre_pais,
            rp.nombre_region,
            rp.fecha_carga as fecha_actual,
            rp.origen_pais as origen
        from {{ source("raw", "LNK_REGIONES_PAISES") }} rp
        join {{ source("raw", "SAT_PAISES") }} sp on rp.hub_pais_id = sp.hub_pais_id
        where
            sp.fecha_carga = (
                select max(sp2.fecha_carga)
                from {{ source("raw", "SAT_PAISES") }} sp2
                where sp2.hub_pais_id = sp.hub_pais_id
            )

    )
{% if is_incremental() %}
        ,
        filtrado as (
            select s.*
            from dim1_paises s
            left join
                {{ this }} t
                on s.dim1_pais_id = t.dim1_pais_id
                and s.dim1_region_id = t.dim1_region_id
                and s.nombre_pais = t.nombre_pais
                and s.nombre_region = t.nombre_region
                and s.fecha_carga = t.fecha_carga
            where t.dim1_pais_id is null
        )
    select *
    from filtrado

{% else %} select * from dim1_paises

{% endif %}
