select
    md5(
        upper(trim(nvl(hc.cuenta_balance, '')))
        || upper(trim(nvl(hc.segmento_marketing, '')))
        || upper(
            trim(nvl(hc.c_origen, '')) || to_char(sc.fecha_carga, 'YYYY-MM-DD')
        ) as dim2_cliente_id
    )
    hc.cuenta_balance,
    hc.segmento_marketing,
    hc.c_origen as origen,
    sc.fecha_carga as fecha_actual,
    lead(sc.fecha_carga) over (
        partition by sc.hub_cliente_id order by sc.fecha_carga
    ) as fecha_final
from
    {{ source("raw", "HUB_CLIENTES") }} hc,
    {{ source("raw", "SAT_CLIENTES_CUENTAS") }} sc
where hc.hub_cliente_id = sc.hub_cliente_id
