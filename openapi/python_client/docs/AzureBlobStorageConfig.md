# AzureBlobStorageConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | [optional] 
**connection_string** | **str** |  | [optional] 
**container_name** | **str** |  | [optional] 
**name_starts_with** | **str** |  | [optional] 

## Example

```python
from cohere.compass.models.azure_blob_storage_config import AzureBlobStorageConfig

# TODO update the JSON string below
json = "{}"
# create an instance of AzureBlobStorageConfig from a JSON string
azure_blob_storage_config_instance = AzureBlobStorageConfig.from_json(json)
# print the JSON string representation of the object
print(AzureBlobStorageConfig.to_json())

# convert the object into a dict
azure_blob_storage_config_dict = azure_blob_storage_config_instance.to_dict()
# create an instance of AzureBlobStorageConfig from a dict
azure_blob_storage_config_from_dict = AzureBlobStorageConfig.from_dict(azure_blob_storage_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


