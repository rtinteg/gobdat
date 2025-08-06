{{ config(materialized="incremental", unique_key="dim2_cliente_id") }}

with
    sat_clientes_cuenta as (
        select
            sc.hub_cliente_id,
            hc.nombre_cliente,
            sc.segmento_marketing,
            sc.c_origen as origen,
            sc.fecha_carga as fecha_inicial_validez,
            lead(sc.fecha_carga) over (
                partition by sc.hub_cliente_id order by sc.fecha_carga
            ) as fecha_final_validez
        from {{ source("raw", "SAT_CLIENTES_CUENTA") }} sc
        join
            {{ source("raw", "HUB_CLIENTES") }} hc
            on hc.hub_cliente_id = sc.hub_cliente_id
    ),
    con_ids as (
        select
            md5(
                upper(trim(nvl(nombre_cliente, '')))
                || to_char(fecha_inicial_validez, 'YYYY-MM-DD')
            ) as dim2_cliente_id,
            nombre_cliente,
            segmento_marketing,
            origen,
            fecha_inicial_validez,
            fecha_final_validez
        from sat_clientes_cuenta
    )
{% if is_incremental() %}
        ,
        filtrado as (
            select c.*
            from con_ids c
            left join {{ this }} t on c.dim2_cliente_id = t.dim2_cliente_id
            where t.dim2_cliente_id is null
        )
    select *
    from filtrado

{% else %} select * from con_ids

{% endif %}
