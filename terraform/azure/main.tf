terraform {
  required_version = ">= 1.6"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.38"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "stterraformstateleandro"
    container_name       = "tfstate"
    key                  = "databricks-platform/azure.tfstate"
  }
}

# ---- providers ----

provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = true
    }
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
  }
}

# provider databricks usa o workspace recém-criado
provider "databricks" {
  alias = "workspace"
  host  = azurerm_databricks_workspace.main.workspace_url
}

# ---- resource group ----

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location

  tags = local.tags
}

# ---- ADLS Gen2 (storage account) ----

resource "azurerm_storage_account" "datalake" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "GRS"    # geo-redundant para produção
  account_kind             = "StorageV2"
  is_hns_enabled           = true     # habilita ADLS Gen2 (Hierarchical Namespace)
  min_tls_version          = "TLS1_2"

  # bloqueia acesso anônimo
  allow_nested_items_to_be_public = false

  blob_properties {
    versioning_enabled  = true
    change_feed_enabled = true    # necessário para Auto Loader em modo SQS-like

    delete_retention_policy {
      days = 30
    }
    container_delete_retention_policy {
      days = 7
    }
  }

  tags = local.tags
}

# containers (filesystems) do ADLS
resource "azurerm_storage_data_lake_gen2_filesystem" "raw" {
  name               = "raw"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "checkpoints" {
  name               = "checkpoints"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "quarantine" {
  name               = "quarantine"
  storage_account_id = azurerm_storage_account.datalake.id
}

# ---- VNet para private endpoints ----

resource "azurerm_virtual_network" "main" {
  name                = "vnet-databricks-${var.environment}"
  address_space       = ["10.179.0.0/16"]
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  tags = local.tags
}

resource "azurerm_subnet" "public" {
  name                 = "snet-databricks-public"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.179.0.0/18"]

  delegation {
    name = "databricks-delegation"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

resource "azurerm_subnet" "private" {
  name                 = "snet-databricks-private"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.179.64.0/18"]

  delegation {
    name = "databricks-delegation"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

resource "azurerm_subnet" "pe" {
  name                                      = "snet-private-endpoints"
  resource_group_name                       = azurerm_resource_group.main.name
  virtual_network_name                      = azurerm_virtual_network.main.name
  address_prefixes                          = ["10.179.128.0/24"]
  private_endpoint_network_policies_enabled = false
}

# NSG obrigatório para subnets Databricks
resource "azurerm_network_security_group" "databricks" {
  name                = "nsg-databricks-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.tags
}

resource "azurerm_subnet_network_security_group_association" "public" {
  subnet_id                 = azurerm_subnet.public.id
  network_security_group_id = azurerm_network_security_group.databricks.id
}

resource "azurerm_subnet_network_security_group_association" "private" {
  subnet_id                 = azurerm_subnet.private.id
  network_security_group_id = azurerm_network_security_group.databricks.id
}

# ---- Databricks Workspace Premium ----

resource "azurerm_databricks_workspace" "main" {
  name                        = "dbw-${var.project_name}-${var.environment}"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  sku                         = "premium"    # necessário para Unity Catalog e Private Link
  managed_resource_group_name = "rg-databricks-managed-${var.environment}"

  public_network_access_enabled         = false    # força uso do Private Link
  network_security_group_rules_required = "AllRules"

  custom_parameters {
    virtual_network_id                                   = azurerm_virtual_network.main.id
    public_subnet_name                                   = azurerm_subnet.public.name
    private_subnet_name                                  = azurerm_subnet.private.name
    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.private.id
    storage_account_name                                 = "dbwstorage${var.environment}"
    storage_account_sku_name                             = "Standard_GRS"
  }

  tags = local.tags
}

# ---- Private Endpoints ----

resource "azurerm_private_endpoint" "databricks_ui" {
  name                = "pe-dbw-ui-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  subnet_id           = azurerm_subnet.pe.id

  private_service_connection {
    name                           = "psc-dbw-ui"
    private_connection_resource_id = azurerm_databricks_workspace.main.id
    subresource_names              = ["browser_authentication"]
    is_manual_connection           = false
  }

  tags = local.tags
}

resource "azurerm_private_endpoint" "databricks_backend" {
  name                = "pe-dbw-backend-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  subnet_id           = azurerm_subnet.pe.id

  private_service_connection {
    name                           = "psc-dbw-backend"
    private_connection_resource_id = azurerm_databricks_workspace.main.id
    subresource_names              = ["databricks_ui_api"]
    is_manual_connection           = false
  }

  tags = local.tags
}

# ---- Unity Catalog Metastore ----

# Access Connector para Unity Catalog (Workload Identity)
resource "azurerm_databricks_access_connector" "unity" {
  name                = "ac-unity-catalog-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  identity {
    type = "SystemAssigned"    # Managed Identity gerenciada pelo Azure
  }

  tags = local.tags
}

# permissão do Access Connector no ADLS (Storage Blob Data Contributor)
resource "azurerm_role_assignment" "unity_adls" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity.identity[0].principal_id
}

# metastore do Unity Catalog
resource "databricks_metastore" "main" {
  provider      = databricks.workspace
  name          = "metastore-${var.environment}"
  storage_root  = "abfss://unity@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
  owner         = var.metastore_admin_group
  force_destroy = false

  depends_on = [azurerm_role_assignment.unity_adls]
}

resource "databricks_metastore_assignment" "main" {
  provider     = databricks.workspace
  metastore_id = databricks_metastore.main.id
  workspace_id = azurerm_databricks_workspace.main.workspace_id
}

# ---- locals ----

locals {
  tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
    owner       = "leandro.moraes"
  }
}
