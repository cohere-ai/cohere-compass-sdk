# CreateDataSource


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**datasource** | [**DataSource**](DataSource.md) |  | [optional] 
**state_key** | **str** |  | [optional] 

## Example

```python
from cohere.compass.models.create_data_source import CreateDataSource

# TODO update the JSON string below
json = "{}"
# create an instance of CreateDataSource from a JSON string
create_data_source_instance = CreateDataSource.from_json(json)
# print the JSON string representation of the object
print(CreateDataSource.to_json())

# convert the object into a dict
create_data_source_dict = create_data_source_instance.to_dict()
# create an instance of CreateDataSource from a dict
create_data_source_from_dict = CreateDataSource.from_dict(create_data_source_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


