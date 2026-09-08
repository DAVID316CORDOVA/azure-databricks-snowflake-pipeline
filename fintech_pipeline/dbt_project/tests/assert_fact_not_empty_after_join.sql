-- tests/assert_fact_not_empty_after_join.sql
--
-- Detecta el caso especifico que preguntaste: "que pasa si un join deja
-- la tabla final vacia (o casi)". No_null/relationships (en schema.yml)
-- detectan filas individuales huerfanas; este test detecta el caso mas
-- grave -- una perdida masiva de filas por un join mal escrito (ej. un
-- INNER JOIN que deberia ser LEFT JOIN, o una condicion de join
-- incorrecta que descarta casi todo).
--
-- fact_customer_activity usa unique_key='customer_id' con merge
-- incremental (ver el .sql del modelo) -- NO hace ningun JOIN, solo
-- filtra bronze_customers incrementalmente y deduplica por customer_id.
-- Por eso comparamos CLIENTES DISTINTOS, no conteo total de filas:
-- bronze_customers trae duplicados a proposito (el generador los
-- produce), asi que bronze.count(*) > fact.count(*) es NORMAL y
-- esperado -- lo que NO es normal es que fact tenga menos clientes
-- UNICOS de los que existen en bronze.

with bronze_distinct_customers as (
    select count(distinct customer_id) as customer_count
    from {{ ref('bronze_customers') }}
),

fact_customers as (
    select count(*) as customer_count
    from {{ ref('fact_customer_activity') }}
)

select
    bronze_distinct_customers.customer_count as bronze_distinct_customers,
    fact_customers.customer_count as fact_rows
from bronze_distinct_customers
cross join fact_customers
where fact_customers.customer_count < (bronze_distinct_customers.customer_count * 0.9)
-- Umbral de 90%, no 50% -- ahora que comparamos clientes UNICOS (no
-- filas crudas con duplicados), un fact con muchos menos clientes
-- distintos que bronze si es una senal real de que algo se rompio
-- (ej. el filtro incremental descartando de mas, o un cambio en la
-- logica de merge).