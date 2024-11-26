# DatasourceConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | [optional] 
**connection_string** | **str** |  | [optional] 
**container_name** | **str** |  | [optional] 
**name_starts_with** | **str** |  | [optional] 

## Example

```python
from cohere.compass.models.datasource_config import DatasourceConfig

# TODO update the JSON string below
json = "{}"
# create an instance of DatasourceConfig from a JSON string
datasource_config_instance = DatasourceConfig.from_json(json)
# print the JSON string representation of the object
print(DatasourceConfig.to_json())

# convert the object into a dict
datasource_config_dict = datasource_config_instance.to_dict()
# create an instance of DatasourceConfig from a dict
datasource_config_from_dict = DatasourceConfig.from_dict(datasource_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


