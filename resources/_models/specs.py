from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Layer = Literal["raw", "base", "staging", "final", "full"]
PipelineKind = Literal["single_layer", "full_stack"]
NAME_PATTERN = r"^[a-z][a-z0-9_]*$"


class StrictSpecModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class ResourceSpecBase(StrictSpecModel):
    domain: str = Field(pattern=NAME_PATTERN)
    source: str = Field(pattern=NAME_PATTERN)
    layer: Layer
    asset: str = Field(pattern=NAME_PATTERN)

    @field_validator("domain", "source", "asset")
    @classmethod
    def _must_be_lowercase(cls, value: str) -> str:
        if value != value.lower():
            raise ValueError("must be lowercase snake_case")
        return value

    @property
    def resource_name(self) -> str:
        return f"{self.domain}_{self.source}_{self.layer}_{self.asset}"


class JobSpec(ResourceSpecBase):
    description: str | None = None
    max_concurrent_runs: int | None = None
    trigger: dict[str, Any] | None = None
    schedule: dict[str, Any] | None = None
    email_notifications: dict[str, Any] | None = None
    timeout_seconds: int | None = None
    tags: dict[str, str] = Field(default_factory=dict)
    parameters: list[dict[str, Any]] = Field(default_factory=list)
    job_clusters: list[dict[str, Any]] = Field(default_factory=list)
    tasks: list[dict[str, Any]] = Field(min_length=1)
    environments: list[dict[str, Any]] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_layer(self) -> JobSpec:
        if self.layer == "full":
            raise ValueError("Job specs do not support layer='full'.")
        return self


class PipelineSpec(ResourceSpecBase):
    pipeline_kind: PipelineKind = "single_layer"
    development: bool | None = None
    continuous: bool | None = None
    serverless: bool = True
    channel: str | None = None
    edition: str | None = None
    photon: bool | None = None
    clusters: list[dict[str, Any]] = Field(default_factory=list)
    configuration: dict[str, str] = Field(default_factory=dict)
    libraries: list[dict[str, Any]] = Field(default_factory=list)
    environment: dict[str, Any] | None = None
    tags: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_pipeline_kind(self) -> PipelineSpec:
        if self.pipeline_kind == "full_stack" and self.layer != "full":
            raise ValueError("Full-stack pipelines require layer='full'.")
        if self.pipeline_kind == "single_layer" and self.layer == "full":
            raise ValueError("Single-layer pipelines do not allow layer='full'.")
        return self
