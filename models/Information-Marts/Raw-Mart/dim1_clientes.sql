select
    hc.hub_cliente_id,
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
where
    sc.fecha_carga = (
        select max(sc2.fecha_carga)
        from {{ source("raw", "SAT_CLIENTES_CUENTA") }} sc2
        where sc.hub_cliente_id = sc2.hub_cliente_id
    )
