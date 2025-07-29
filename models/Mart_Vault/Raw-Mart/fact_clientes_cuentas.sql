with
    fact_clientes_cuentas as (
        select
            pit.hub_cliente_id,
            sc.cuenta_balance,
            sc.fecha_carga as fecha,
            sc.c_origen as origen
        from {{ source("business", "PIT_CLIENTES") }} pit
        left join
            {{ source("raw", "SAT_CLIENTES_CUENTA") }} sc
            on sc.hub_cliente_id = pit.hub_cliente_id
            and sc.fecha_carga = pit.fecha_cliente_cuenta
            and sc.fecha_carga = (
                select max(sc2.fecha_carga)
                from {{ source("raw", "SAT_CLIENTES_CUENTA") }} sc2
                where sc.hub_cliente_id = sc2.hub_cliente_id
            )
    )
select *
from fact_clientes_cuentas
