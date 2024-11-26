# cohere.compass.DefaultApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_datasource**](DefaultApi.md#create_datasource) | **POST** /datasources | Create a new datasource
[**delete_datasource**](DefaultApi.md#delete_datasource) | **DELETE** /datasources/{datasource_id} | Delete a datasource by ID
[**get_config**](DefaultApi.md#get_config) | **GET** /datasources/config/{config_type} | Get the configuration schema for a specific config type
[**get_datasource**](DefaultApi.md#get_datasource) | **GET** /datasources/{datasource_id} | Get a datasource by ID
[**list_configs**](DefaultApi.md#list_configs) | **GET** /datasources/config | List all available config types
[**list_datasources**](DefaultApi.md#list_datasources) | **GET** /datasources | List all datasources


# **create_datasource**
> DataSource create_datasource(create_data_source)

Create a new datasource

### Example

* Basic Authentication (basicAuth):
* Bearer (JWT) Authentication (bearerAuth):

```python
import cohere.compass
from cohere.compass.models.create_data_source import CreateDataSource
from cohere.compass.models.data_source import DataSource
from cohere.compass.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = cohere.compass.Configuration(
    host = "http://localhost"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basicAuth
configuration = cohere.compass.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure Bearer authorization (JWT): bearerAuth
configuration = cohere.compass.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with cohere.compass.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = cohere.compass.DefaultApi(api_client)
    create_data_source = cohere.compass.CreateDataSource() # CreateDataSource | 

    try:
        # Create a new datasource
        api_response = api_instance.create_datasource(create_data_source)
        print("The response of DefaultApi->create_datasource:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->create_datasource: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_data_source** | [**CreateDataSource**](CreateDataSource.md)|  | 

### Return type

[**DataSource**](DataSource.md)

### Authorization

[basicAuth](../README.md#basicAuth), [bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Created datasource |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_datasource**
> delete_datasource(datasource_id)

Delete a datasource by ID

### Example

* Basic Authentication (basicAuth):
* Bearer (JWT) Authentication (bearerAuth):

```python
import cohere.compass
from cohere.compass.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = cohere.compass.Configuration(
    host = "http://localhost"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basicAuth
configuration = cohere.compass.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure Bearer authorization (JWT): bearerAuth
configuration = cohere.compass.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with cohere.compass.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = cohere.compass.DefaultApi(api_client)
    datasource_id = 'datasource_id_example' # str | 

    try:
        # Delete a datasource by ID
        api_instance.delete_datasource(datasource_id)
    except Exception as e:
        print("Exception when calling DefaultApi->delete_datasource: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **datasource_id** | **str**|  | 

### Return type

void (empty response body)

### Authorization

[basicAuth](../README.md#basicAuth), [bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Datasource deleted |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_config**
> GetConfigResponse get_config(config_type)

Get the configuration schema for a specific config type

### Example

* Basic Authentication (basicAuth):
* Bearer (JWT) Authentication (bearerAuth):

```python
import cohere.compass
from cohere.compass.models.get_config_response import GetConfigResponse
from cohere.compass.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = cohere.compass.Configuration(
    host = "http://localhost"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basicAuth
configuration = cohere.compass.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure Bearer authorization (JWT): bearerAuth
configuration = cohere.compass.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with cohere.compass.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = cohere.compass.DefaultApi(api_client)
    config_type = 'config_type_example' # str | 

    try:
        # Get the configuration schema for a specific config type
        api_response = api_instance.get_config(config_type)
        print("The response of DefaultApi->get_config:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_config: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **config_type** | **str**|  | 

### Return type

[**GetConfigResponse**](GetConfigResponse.md)

### Authorization

[basicAuth](../README.md#basicAuth), [bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Configuration schema and authentication URL |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_datasource**
> DataSource get_datasource(datasource_id)

Get a datasource by ID

### Example

* Basic Authentication (basicAuth):
* Bearer (JWT) Authentication (bearerAuth):

```python
import cohere.compass
from cohere.compass.models.data_source import DataSource
from cohere.compass.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = cohere.compass.Configuration(
    host = "http://localhost"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basicAuth
configuration = cohere.compass.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure Bearer authorization (JWT): bearerAuth
configuration = cohere.compass.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with cohere.compass.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = cohere.compass.DefaultApi(api_client)
    datasource_id = 'datasource_id_example' # str | 

    try:
        # Get a datasource by ID
        api_response = api_instance.get_datasource(datasource_id)
        print("The response of DefaultApi->get_datasource:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_datasource: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **datasource_id** | **str**|  | 

### Return type

[**DataSource**](DataSource.md)

### Authorization

[basicAuth](../README.md#basicAuth), [bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Datasource details |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_configs**
> List[str] list_configs()

List all available config types

### Example

* Basic Authentication (basicAuth):
* Bearer (JWT) Authentication (bearerAuth):

```python
import cohere.compass
from cohere.compass.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = cohere.compass.Configuration(
    host = "http://localhost"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basicAuth
configuration = cohere.compass.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure Bearer authorization (JWT): bearerAuth
configuration = cohere.compass.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with cohere.compass.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = cohere.compass.DefaultApi(api_client)

    try:
        # List all available config types
        api_response = api_instance.list_configs()
        print("The response of DefaultApi->list_configs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->list_configs: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

**List[str]**

### Authorization

[basicAuth](../README.md#basicAuth), [bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | A list of available config types |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_datasources**
> PaginatedListDataSource list_datasources()

List all datasources

### Example

* Basic Authentication (basicAuth):
* Bearer (JWT) Authentication (bearerAuth):

```python
import cohere.compass
from cohere.compass.models.paginated_list_data_source import PaginatedListDataSource
from cohere.compass.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = cohere.compass.Configuration(
    host = "http://localhost"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basicAuth
configuration = cohere.compass.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure Bearer authorization (JWT): bearerAuth
configuration = cohere.compass.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with cohere.compass.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = cohere.compass.DefaultApi(api_client)

    try:
        # List all datasources
        api_response = api_instance.list_datasources()
        print("The response of DefaultApi->list_datasources:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->list_datasources: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**PaginatedListDataSource**](PaginatedListDataSource.md)

### Authorization

[basicAuth](../README.md#basicAuth), [bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | A paginated list of datasources |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

