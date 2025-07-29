with
    csv_nuevos as (

        select
            c_acctbal,
            c_address,
            c_comment,
            c_custkey,
            c_mktsegment,
            c_name,
            c_nationkey,
            c_phone,
            c_origen,
            current_timestamp() as fecha_carga
        from {{ source("stg", "CLIENTES_ELT") }}
    ),
    filtrados as (
        select *
        from csv_nuevos
        where c_custkey not in (select c_custkey from {{ source("stg", "STG_CLIENTES") }})
    )
-- select * from  filtrados

    -- Esta tabla no se guarda como `load_clientes_from_csv`, sino que hace INSERT
    -- INTO stg_clientes
    insert into {{ ref("stg_clientes") }} (
        c_acctbal,
        c_address,
        c_comment,
        c_custkey,
        c_mktsegment,
        c_name,
        c_nationkey,
        c_phone,
        c_origen,
        fecha_carga
    )
select
    c_acctbal,
    c_address,
    c_comment,
    c_custkey,
    c_mktsegment,
    c_name,
    c_nationkey,
    c_phone,
    c_origen,
    fecha_carga
from filtrados
