# GetConfigResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**config_schema** | **object** |  | [optional] 
**auth_url** | **str** |  | [optional] 

## Example

```python
from cohere.compass.models.get_config_response import GetConfigResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetConfigResponse from a JSON string
get_config_response_instance = GetConfigResponse.from_json(json)
# print the JSON string representation of the object
print(GetConfigResponse.to_json())

# convert the object into a dict
get_config_response_dict = get_config_response_instance.to_dict()
# create an instance of GetConfigResponse from a dict
get_config_response_from_dict = GetConfigResponse.from_dict(get_config_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


