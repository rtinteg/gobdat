-- Tabla PIT PIT_CLIENTES para la obtención eficiente del histórico persistido en las tablas satelites de Cliente 
select
    h.hub_cliente_id,
    current_date as pit_fecha,
    s1.fecha_carga as fecha_cliente_contacto,
    s2.fecha_carga as fecha_cliente_cuenta
from {{ source("raw", "HUB_CLIENTES") }} h
join
    (
        select *
        from {{ source("raw", "SAT_CLIENTES_CONTACTO") }}
        where
            (hub_cliente_id, fecha_carga) in (
                select hub_cliente_id, max(fecha_carga)
                from {{ source("raw", "SAT_CLIENTES_CONTACTO") }}
                group by hub_cliente_id
            )
    ) s1
    on h.hub_cliente_id = s1.hub_cliente_id
join
    (
        select *
        from {{ source("raw", "SAT_CLIENTES_CUENTA") }}
        where
            (hub_cliente_id, fecha_carga) in (
                select hub_cliente_id, max(fecha_carga)
                from {{ source("raw", "SAT_CLIENTES_CUENTA") }}
                group by hub_cliente_id
            )
    ) s2
    on h.hub_cliente_id = s2.hub_cliente_id
