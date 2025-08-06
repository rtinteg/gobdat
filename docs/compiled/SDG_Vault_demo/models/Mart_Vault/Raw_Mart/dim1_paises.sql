

with
    dim1_paises as (
        select
            rp.hub_pais_id as dim1_pais_id,
            rp.hub_region_id as dim1_region_id,
            rp.nombre_pais,
            rp.nombre_region,
            rp.fecha_carga as fecha_actual,
            rp.origen_pais as origen
        from SDGVAULTMART.DBT_SDGVAULT_BRONZE.LNK_REGIONES_PAISES rp
        join SDGVAULTMART.DBT_SDGVAULT_BRONZE.SAT_PAISES sp on rp.hub_pais_id = sp.hub_pais_id
        where
            sp.fecha_carga = (
                select max(sp2.fecha_carga)
                from SDGVAULTMART.DBT_SDGVAULT_BRONZE.SAT_PAISES sp2
                where sp2.hub_pais_id = sp.hub_pais_id
            )

    )

        ,
        filtrado as (
            select s.*
            from dim1_paises s
            left join
                SDGVAULTMART.DBT_SDGVAULT_SILVER.dim1_paises t
                on s.dim1_pais_id = t.dim1_pais_id
                and s.dim1_region_id = t.dim1_region_id
                and s.nombre_pais = t.nombre_pais
                and s.nombre_region = t.nombre_region
                and s.fecha_actual = t.fecha_actual
            where t.dim1_pais_id is null
        )
    select *
    from filtrado

