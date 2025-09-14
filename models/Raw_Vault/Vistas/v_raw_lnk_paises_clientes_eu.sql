{{ config(materialized="view") }}

select *
from {{ source("raw", "LNK_PAISES_CLIENTES") }}
where
    nombre_pais
    in ('ESPAÑA', 'GERMANY', 'RUSSIA', 'ROMANIA', 'UNITED KINGDOM', 'FRANCE', 'ITALIA')
