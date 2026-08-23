## =================================================================
## providers.tf
## Two providers: azurerm for ADLS Gen2/Data Factory/Databricks
## workspace, and snowflake for the warehouse that will receive the
## Bronze-layer data cleaned by Databricks.
##
## Authentication: azurerm reads ARM_CLIENT_ID, ARM_CLIENT_SECRET,
## ARM_TENANT_ID, ARM_SUBSCRIPTION_ID from environment variables
## (loaded via `source .env`) -- never hardcoded here.
## =================================================================

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.95"
    }
  }
}

provider "azurerm" {
  features {}
}

provider "snowflake" {
  organization_name = "MZJLBCT"
  account_name      = "CUC08629"
  user              = "david"
  password          = var.snowflake_password
  role              = "ACCOUNTADMIN"
}
