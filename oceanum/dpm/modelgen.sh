#!/bin/bash
DPM_API_URL=${DPM_API_URL:-http://localhost:8000/api/}
datamodel-codegen \
    --url "${DPM_API_URL}openapi.json" \
    --input-file-type=openapi \
    --output=models.py \
    --target-python-version=3.11 \
    --output-model-type=pydantic_v2.BaseModel \
    --snake-case-field \
    --use-default-kwarg \
    --reuse-model \
    --use-standard-collections \
    --strict-nullable \
    --use-schema-description \
    --field-constraints \
    --use-one-literal-as-default \
    --disable-timestamp
