

select lcp.*
from SDGVAULTMART.DBT_SDGVAULT_BRONZE.LNK_CLIENTES_PEDIDOS lcp
join
    SDGVAULTMART.DBT_SDGVAULT_BRONZE.LNK_PAISES_CLIENTES lpc
    on lcp.hub_cliente_id = lpc.hub_cliente_id
    and lpc.nombre_pais
    in ('ESPAÑA', 'GERMANY', 'RUSSIA', 'ROMANIA', 'UNITED KINGDOM', 'FRANCE')