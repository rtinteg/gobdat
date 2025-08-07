{{ config(materialized="incremental", unique_key="dim2_cliente_id") }}

with
    fuente as (

        select
            sc.hub_cliente_id,
            hc.nombre_cliente,
            sc.segmento_marketing,
            sc.c_origen as origen,
            sc.fecha_carga as fecha_inicial_validez
        from {{ source("raw", "SAT_CLIENTES_CUENTA") }} sc
        join
            {{ source("raw", "HUB_CLIENTES") }} hc
            on sc.hub_cliente_id = hc.hub_cliente_id

    )

{% if not is_incremental() %}

        ,
        -- PRIMERA CARGA: full load con lead() para calcular fecha_final_validez
        ordenado as (
            select
                f.*,
                lead(f.fecha_inicial_validez) over (
                    partition by f.hub_cliente_id order by f.fecha_inicial_validez
                ) as fecha_final_validez
            from fuente f
        )

    select
        md5(
            upper(trim(coalesce(nombre_cliente, '')))
            || trim(coalesce(segmento_marketing, ''))
            || cast(fecha_inicial_validez as string)
        ) as dim2_cliente_id,
        hub_cliente_id,
        nombre_cliente,
        segmento_marketing,
        origen,
        fecha_inicial_validez,
        fecha_final_validez

    from ordenado

{% else %}

        ,
        -- INCREMENTAL: detectar cambios y versionar
        -- Último registro insertado por cliente
        registro_actual as (
            select *
            from {{ this }}
            qualify
                row_number() over (
                    partition by hub_cliente_id order by fecha_inicial_validez desc
                )
                = 1
        ),
        -- Nuevos datos desde el origen
        nuevos_datos as (select * from fuente),
        -- Detectar cambios en valores comparables
        cambios as (
            select
                n.hub_cliente_id,
                n.nombre_cliente,
                n.segmento_marketing,
                n.origen,
                n.fecha_inicial_validez,
                null as fecha_final_validez,
                md5(
                    upper(trim(coalesce(n.nombre_cliente, '')))
                    || trim(coalesce(n.segmento_marketing, ''))
                    || cast(n.fecha_inicial_validez as string)
                ) as dim2_cliente_id
            from nuevos_datos n
            left join registro_actual r on n.hub_cliente_id = r.hub_cliente_id
            where
                r.hub_cliente_id is null
                or r.segmento_marketing != n.segmento_marketing
                or r.nombre_cliente != n.nombre_cliente
        ),
        -- Actualizar fecha_final_validez en la versión anterior
        cerrar_versiones as (
            select
                r.dim2_cliente_id,
                r.hub_cliente_id,
                r.nombre_cliente,
                r.segmento_marketing,
                r.origen,
                r.fecha_inicial_validez,
                c.fecha_inicial_validez as fecha_final_validez
            from registro_actual r
            join cambios c on r.hub_cliente_id = c.hub_cliente_id
        )

    -- Unión: nuevas versiones + cierre versiones anteriores
    select *
    from cambios
    union all
    select
        md5(
            upper(trim(coalesce(r.nombre_cliente, '')))
            || trim(coalesce(r.segmento_marketing, ''))
            || cast(r.fecha_inicial_validez as string)
        ) as dim2_cliente_id,
        hub_cliente_id,
        nombre_cliente,
        segmento_marketing,
        origen,
        fecha_inicial_validez,
        fecha_final_validez
    from cerrar_versiones r

{% endif %}

