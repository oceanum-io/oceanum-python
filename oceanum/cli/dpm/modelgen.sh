#!/bin/bash
datamodel-codegen \
    --url "${1}openapi.json" \
    --input-file-type openapi \
    --output models.py \
    --target-python-version "3.10" \
    --output-model-type pydantic_v2.BaseModel \
    --snake-case-field \
    --use-default-kwarg \
    --reuse-model \
    --use-standard-collections \
    --strict-nullable \
    --use-schema-description \
    --field-constraints \
    --use-one-literal-as-default \
    --disable-timestamp
