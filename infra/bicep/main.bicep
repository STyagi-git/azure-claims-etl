targetScope = 'resourceGroup'

param prefix string = 'claimsde'
param location string = resourceGroup().location

resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: '${prefix}stg'
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

resource adf 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: '${prefix}-adf'
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {}
}

resource dbw 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: '${prefix}-dbw'
  location: location
  sku: { name: 'standard' }
  properties: {
    managedResourceGroupId: '/subscriptions/${subscription().subscriptionId}/resourceGroups/${prefix}-dbw-mrg'
  }
}

output storageName string = storage.name
output adfName string = adf.name
output databricksWorkspace string = dbw.name
