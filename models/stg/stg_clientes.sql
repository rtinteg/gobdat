{{ config(
    materialized = 'table'
) }}

with stg_clientes as (
    select *
    from {{ source('stg', 'CUSTOMER') }}
)
select * from stg_clientes