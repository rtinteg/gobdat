{{
    config(
        materialized="incremental",
        unique_key="dim1_cliente_id",
        merge_update_columns=[
            "nombre_cliente",
            "segmento_marketing",
            "fecha",
            "origen",
        ],
    )
}}

with
    dim1_clientes as (
        select
            hc.hub_cliente_id as dim1_cliente_id,
            hc.nombre_cliente,
            sc.segmento_marketing,
            sc.fecha_carga as fecha,
            sc.c_origen as origen
        from {{ source("raw", "HUB_CLIENTES") }} hc
        join
            {{ source("business", "PIT_CLIENTES") }} pit
            on hc.hub_cliente_id = pit.hub_cliente_id
        left join
            {{ source("raw", "SAT_CLIENTES_CUENTA") }} sc
            on sc.hub_cliente_id = pit.hub_cliente_id
            and sc.fecha_carga = pit.fecha_cliente_cuenta
            and sc.fecha_carga = (
                select max(sc2.fecha_carga)
                from {{ source("raw", "SAT_CLIENTES_CUENTA") }} sc2
                where sc.hub_cliente_id = sc2.hub_cliente_id
            )
    ),
    filtrado as (
        select s.*
        from dim1_clientes s
        left join
            {{ this }} t
            on s.dim1_cliente_id = t.dim1_cliente_id
            and s.nombre_cliente = t.nombre_cliente
            and s.segmento_marketing = t.segmento_marketing
            and s.fecha = t.fecha
            and s.origen = t.origen
        where t.dim1_cliente_id is null
    )
select *
from filtrado
