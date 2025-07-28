select
    md5(
        upper(trim(nvl(hc.nombre_cliente, ''))) || to_char(sc.fecha_carga, 'YYYY-MM-DD')
    ) as dim2_cliente_id,
    hc.nombre_cliente,
    sc.segmento_marketing,
    sc.c_origen as origen,
    sc.fecha_carga as fecha_inicial_validez,
    lead(sc.fecha_carga) over (
        partition by sc.hub_cliente_id order by sc.fecha_carga
    ) as fecha_final_validez
from
    {{ source("raw", "HUB_CLIENTES") }} hc,
    {{ source("raw", "SAT_CLIENTES_CUENTA") }} sc
where hc.hub_cliente_id = sc.hub_cliente_id
