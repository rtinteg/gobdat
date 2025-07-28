select hc.hub_cliente_id, hc.nombre_cliente, sc.c_origen, sc.segmento_marketing
from {{ source("raw", "HUB_CLIENTES") }} hc,
join
    {{ source("business", "PIT_CLIENTES") }} pit
    on hc.hub_cliente_id = pit.hub_cliente_id
left join
    {{ source("raw", "SAT_CLIENTES_CUENTA") }} sc
    on sc.hub_cliente_id = pit.hub_cliente_id
    and sc.fecha_carga = pit.fecha_cliente_cuenta
