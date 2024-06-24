# Cohere Compass SDK 

The Compass SDK is a Python library that allows you to parse documents and insert them into a Compass index.

In order to parser documents, the Compass SDK relies on the Compass Parser API, which is a RESTful API that
receives files and returns parsed documents. The parser API is provided as a dockerized service that can be
deployed locally or in the cloud.

The Compass SDK provides a `CompassParserClient` that allows to interact with the parser API from your
Python code in a convenient manner. The `CompassParserClient` provides methods to parse single and multiple
files, as well as entire folders, and supports multiple file types (e.g., `pdf`, `docx`, `json`, `csv`, etc.) as well
as different file systems (e.g., local, S3, GCS, etc.).

To insert parsed documents into a `Compass` index, the Compass SDK provides a `CompassClient` class that
allows to interact with a Compass API server. The Compass API is also a RESTful API that allows to create,
delete and search documents in a Compass index. To install a Compass API service, please refer to the
[Compass documentation](https://github.com/cohere-ai/compass)
