## =================================================================
## adf.tf
## Azure Data Factory: orchestrates the initial copy of the dirty
## JSON dataset into the "raw" container. No Airflow in this
## project -- ADF is the native orchestrator, used deliberately to
## practice the idiomatic tool of this cloud instead of repeating
## Airflow (already mastered in the companion AWS project).
## =================================================================

resource "azurerm_data_factory" "fintech" {
  name                = "adf-fintech-fdcg01"
  location            = azurerm_resource_group.fintech.location
  resource_group_name = azurerm_resource_group.fintech.name

  # System-assigned managed identity: lets ADF authenticate to other
  # Azure resources (like the storage account below) without any
  # stored secret -- Azure itself manages and rotates the identity.
  identity {
    type = "SystemAssigned"
  }
}

# Linked service: how ADF authenticates to the storage account.
# Uses a managed identity -- ADF's own system-assigned identity --
# instead of an access key, so no storage secret needs to be
# embedded in the linked service configuration.
resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "fintech" {
  name                 = "ls_adls_fintech"
  data_factory_id      = azurerm_data_factory.fintech.id
  use_managed_identity = true
  url                  = azurerm_storage_account.fintech.primary_dfs_endpoint
}

# Dataset pointing at the "raw" container -- the destination for the
# locally-generated dirty JSON file.
resource "azurerm_data_factory_dataset_binary" "raw_landing" {
  name                = "ds_raw_customers"
  data_factory_id     = azurerm_data_factory.fintech.id
  linked_service_name = azurerm_data_factory_linked_service_data_lake_storage_gen2.fintech.name

  azure_blob_storage_location {
    container = azurerm_storage_container.raw.name
    filename  = "raw_customers.json"
  }
}

# Grant ADF's managed identity the "Storage Blob Data Contributor"
# role on the storage account, so it can actually read/write files
# there (having a linked service alone isn't enough -- RBAC is
# separate from connectivity).
resource "azurerm_role_assignment" "adf_storage_access" {
  scope                = azurerm_storage_account.fintech.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.fintech.identity[0].principal_id
}