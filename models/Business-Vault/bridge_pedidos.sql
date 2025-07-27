select
md5(
        upper(trim(nvl(hp.clave_pedido, '')))
        || upper(trim(nvl(hp.empleado, '')))
        || upper(trim(nvl(hc.nombre_cliente, '')))
        || upper(trim(nvl(hn.nombre_pais, '')))
    ) as brigde_pedido_id,
    current_date fecha_carga,
    hp.hub_pedido_id,
    hp.clave_pedido,
    hp.empleado,
    lcp.lnk_cliente_pedido_id,
    hc.hub_cliente_id,
    hc.nombre_cliente,
    lpc.lnk_pais_cliente_id,
    hn.hub_pais_id,
    hn.nombre_pais
from {{ source("raw", "HUB_PEDIDOS") }} hp
join {{ ref("lnk_clientes_pedidos") }} lcp on hp.hub_pedido_id = lcp.hub_pedido_id
join {{ source("raw", "HUB_CLIENTES") }} hc on hc.hub_cliente_id = lcp.hub_cliente_id
join {{ ref('lnk_paises_clientes') }} lpc on lpc.hub_cliente_id = hc.hub_cliente_id
join {{ source("raw", "HUB_PAISES") }} hn on hn.hub_pais_id = lpc.hub_pais_id
