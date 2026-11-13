"""
Optional extensions: higher-level features built on the Compass clients.

The core SDK holds the API clients and models; extensions build on them and
are never imported by the core. Each extension that needs more dependencies
has its own extra, for example `pip install cohere-compass-sdk[uploads]` for
`cohere_compass.ext.uploads`, and `pip install cohere-compass-sdk[all]`
installs every extension's dependencies.
"""
