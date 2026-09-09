"""
Genera el sitio HTML de Great Expectations Data Docs contra una copia
local del Parquet exportado por la tarea 'export_to_clean' del Job de
Databricks. Corre esto en tu PC (no dentro de Databricks) -- es
independiente del notebook validate_with_great_expectations.py, que
valida programáticamente sin dejar un sitio persistido.

Requisitos:
    pip install great-expectations pandas pyarrow

Antes de correr:
    1. Descarga una copia del Parquet mas reciente desde ADLS
       (/clean/{dev|prod}/) a tu maquina local, o usa la salida de un
       export_to_clean.py corrido localmente contra una muestra.
    2. Ajusta PARQUET_PATH abajo.
    3. IMPORTANTE: alinea las expectativas de este script con las ~15
       reales que ya tienes en
       fintech_pipeline/src/validate_with_great_expectations.py -- este
       script trae un subconjunto de ejemplo cubriendo las mismas
       categorias (nulls, unicidad, rangos, sets, regex, estadisticas),
       pero no es una copia exacta linea por linea de tu notebook.
"""

import great_expectations as gx
from great_expectations.checkpoint import UpdateDataDocsAction
import pandas as pd

# --- Configuracion ------------------------------------------------------
PARQUET_PATH = "downloaded_parquet/part-00000-tid-7339623126031248017-56af599b-678f-471f-9eb6-85734c59637e-5-1-c000.snappy.parquet"
PROJECT_ROOT = "./ge_docs_project"        # se crea si no existe

# --- 1. Data Context ------------------------------------------------------
context = gx.get_context(mode="file", project_root_dir=PROJECT_ROOT)

# --- 2. Data Source + Data Asset (pandas, sobre el Parquet ya exportado) --
data_source = context.data_sources.add_pandas(name="local_clean_parquet")
data_asset = data_source.add_dataframe_asset(name="clean_customers")
batch_definition = data_asset.add_batch_definition_whole_dataframe("full_batch")

df = pd.read_parquet(PARQUET_PATH)

# En la tabla de Databricks, risk_score vive anidado dentro de 'metadata'
# (a diferencia de Snowflake, donde dbt ya lo aplana en bronze_customers.sql).
# Lo aplanamos aqui, igual que ya haces en validate_with_great_expectations.py
if "metadata" in df.columns:
    df["risk_score"] = df["metadata"].apply(
        lambda m: m.get("risk_score") if isinstance(m, dict) else None
    )

# --- 3. Expectation Suite -- alinea esto con tu notebook real -------------
suite = context.suites.add(gx.ExpectationSuite(name="fintech_customers_suite"))

# Existencia de columnas
suite.add_expectation(
    gx.expectations.ExpectTableColumnsToMatchSet(
        column_set=[
            "customer_id", "customer_name", "age", "country", "email",
            "registration_date", "account_type", "account_balance",
            "kyc_status", "device", "risk_score",
        ],
        exact_match=False,
    )
)

# Volumen
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
)

# Nulls (clave primaria)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
)

# Unicidad
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="customer_id")
)

# Rango numerico
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="age", min_value=18, max_value=100
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="account_balance", min_value=0
    )
)

# Pertenencia a conjunto
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="kyc_status", value_set=["pending", "verified", "rejected"]
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="account_type", value_set=["checking", "savings", "credit"]
    )
)

# Patron de string (regex de email)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchRegex(
        column="email", regex=r"^[\w\.-]+@[\w\.-]+\.\w+$"
    )
)

# Estadisticas de distribucion
suite.add_expectation(
    gx.expectations.ExpectColumnMeanToBeBetween(
        column="age", min_value=18, max_value=100
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnStdevToBeBetween(
        column="age", min_value=1
    )
)

# --- 4. Validation Definition + Checkpoint (con UpdateDataDocsAction) -----
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="fintech_validation",
        data=batch_definition,
        suite=suite,
    )
)

checkpoint = context.checkpoints.add(
    gx.Checkpoint(
        name="fintech_checkpoint",
        validation_definitions=[validation_definition],
        actions=[UpdateDataDocsAction(name="update_data_docs")],
    )
)

# --- 5. Correr el checkpoint -- esto genera el sitio Data Docs -----------
result = checkpoint.run(batch_parameters={"dataframe": df})
print(f"Validation success: {result.success}")

# --- 6. Abrir el sitio en el navegador ------------------------------------
context.open_data_docs()
# El sitio queda en: ./ge_docs_project/gx/uncommitted/data_docs/local_site/index.html