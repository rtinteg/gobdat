{{ config(materialized="view") }}

select *
from {{ source("business", "BRIDGE_PEDIDOS") }}
where
    nombre_pais
    in ('ESPAÑA', 'GERMANY', 'RUSSIA', 'ROMANIA', 'UNITED KINGDOM', 'FRANCE', 'ITALIA')
