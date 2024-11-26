# PaginatedListDataSource


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**value** | [**List[DataSource]**](DataSource.md) |  | [optional] 
**next_page_token** | **str** |  | [optional] 

## Example

```python
from cohere.compass.models.paginated_list_data_source import PaginatedListDataSource

# TODO update the JSON string below
json = "{}"
# create an instance of PaginatedListDataSource from a JSON string
paginated_list_data_source_instance = PaginatedListDataSource.from_json(json)
# print the JSON string representation of the object
print(PaginatedListDataSource.to_json())

# convert the object into a dict
paginated_list_data_source_dict = paginated_list_data_source_instance.to_dict()
# create an instance of PaginatedListDataSource from a dict
paginated_list_data_source_from_dict = PaginatedListDataSource.from_dict(paginated_list_data_source_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


