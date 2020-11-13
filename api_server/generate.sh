#!/usr/bin/env bash
docker run --rm -v "${PWD}":/local openapitools/openapi-generator-cli generate \
-i /local/openapi_server/openapi/openapi.yaml \
-g python-flask \
-o /local \
--skip-overwrite
