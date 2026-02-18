# -*- coding: utf-8 -*-
"""Base classes for oceanum models."""

from pydantic import BaseModel, ConfigDict


class StrictBaseModel(BaseModel):
    """
    Base model with strict validation that forbids extra fields.

    This prevents silent failures when users make typos in field names.
    Instead of ignoring unknown fields, a ValidationError is raised.
    """

    model_config = ConfigDict(extra="forbid")
