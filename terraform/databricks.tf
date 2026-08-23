## =================================================================
## databricks.tf
## Azure Databricks workspace where PySpark cleans the dirty JSON
## before writing results to the "clean" container, which Snowpipe
## will later auto-ingest into Snowflake.
## =================================================================

resource "azurerm_databricks_workspace" "fintech" {
  name                = "dbw-fintech-fdcg01"
  resource_group_name = azurerm_resource_group.fintech.name
  location            = azurerm_resource_group.fintech.location
  sku                 = "premium"
}