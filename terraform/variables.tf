variable "snowflake_password" {
  description = "Snowflake account password, passed as TF_VAR_snowflake_password"
  type        = string
  sensitive   = true
}

variable "resource_group_name" {
  description = "Azure resource group name for this project"
  type        = string
  default     = "rg-fintech-pipeline"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus"
}
