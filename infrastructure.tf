provider "azurerm" {
  features {}
}

data "azurerm_storage_account" "storage" {
  name                     = "datacohortworkspacelabs"
  resource_group_name      = "rg-data-cohort-labs"
}

resource "azurerm_storage_container" "container" {
  count               = length(var.container_names)
  name                = var.container_names[count.index]
  storage_account_name = data.azurerm_storage_account.storage.name
}

variable "container_names" {
  type    = list(string)
  default = ["landing-ibrahim","bronze-ibrahim", "silver-ibrahim", "gold-ibrahim"]
}

