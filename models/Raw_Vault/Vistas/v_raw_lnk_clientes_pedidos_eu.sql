{{ config(materialized="view") }}

select lcp.*
from {{ source("raw", "LNK_CLIENTES_PEDIDOS") }} lcp
join
    {{ source("raw", "LNK_PAISES_CLIENTES") }} lpc
    on lcp.hub_cliente_id = lpc.hub_cliente_id
    and lpc.nombre_pais
    in ('ESPAÑA', 'GERMANY', 'RUSSIA', 'ROMANIA', 'UNITED KINGDOM', 'FRANCE')
