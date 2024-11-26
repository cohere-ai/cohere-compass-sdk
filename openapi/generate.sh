#!/bin/bash
set -e

folder_name=python_client
schema_file=compass_api.json

rm -rf ${folder_name}

docker run --rm -v $PWD:/local openapitools/openapi-generator-cli generate -i /local/${schema_file} -g python -o /local/${folder_name} --package-name cohere.compass

sudo chown -R ${USER}:${USER} ${folder_name}
