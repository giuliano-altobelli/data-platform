from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from resources._generated.generator import (
    build_job_resource,
    build_pipeline_resource,
    build_pipeline_resource_dict,
)
from resources._models.specs import JobSpec, PipelineSpec


def _write_yaml(path: Path, payload: dict[str, object]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def test_job_generation_is_deterministic(tmp_path: Path) -> None:
    spec_path = tmp_path / "job.yml"
    _write_yaml(
        spec_path,
        {
            "domain": "finance",
            "source": "erp",
            "layer": "raw",
            "asset": "ingest_transactions",
            "tasks": [
                {
                    "task_key": "ingest",
                    "notebook_task": {
                        "notebook_path": "src/finance/erp/raw/ingest_transactions.ipynb",
                    },
                }
            ],
        },
    )

    resource_root = tmp_path / "resources"
    first_output_path = build_job_resource(spec_path, resource_root=resource_root)
    first_output = first_output_path.read_text(encoding="utf-8")

    second_output_path = build_job_resource(spec_path, resource_root=resource_root)
    second_output = second_output_path.read_text(encoding="utf-8")

    assert first_output_path == second_output_path
    assert first_output == second_output
    assert "'name': 'finance_erp_raw_ingest_transactions_${bundle.target}'" in first_output
    assert "finance_erp_raw_ingest_transactions = Job.from_dict(" in first_output
    assert "'catalog'" in first_output
    assert "'schema'" in first_output


def test_invalid_spec_rejects_unknown_keys(tmp_path: Path) -> None:
    spec_path = tmp_path / "job-invalid.yml"
    _write_yaml(
        spec_path,
        {
            "domain": "finance",
            "source": "erp",
            "layer": "raw",
            "asset": "ingest_transactions",
            "tasks": [{"task_key": "ingest", "spark_python_task": {"python_file": "main.py"}}],
            "unexpected": "value",
        },
    )

    with pytest.raises(ValidationError):
        build_job_resource(spec_path, resource_root=tmp_path / "resources")


def test_job_spec_rejects_full_layer() -> None:
    with pytest.raises(ValidationError):
        JobSpec(
            domain="finance",
            source="erp",
            layer="full",
            asset="invalid_job_layer",
            tasks=[
                {
                    "task_key": "a",
                    "notebook_task": {"notebook_path": "src/finance/erp/raw/a.ipynb"},
                }
            ],
        )


def test_pipeline_mapping_enforces_catalog_schema_and_layer_paths(tmp_path: Path) -> None:
    spec_path = tmp_path / "pipeline.yml"
    _write_yaml(
        spec_path,
        {
            "domain": "finance",
            "source": "erp",
            "layer": "staging",
            "asset": "customer_360",
        },
    )

    output_path = build_pipeline_resource(spec_path, resource_root=tmp_path / "resources")
    rendered = output_path.read_text(encoding="utf-8")

    assert output_path.name == "finance_erp_staging_customer_360.py"
    assert "'catalog': 'finance'" in rendered
    assert "'name': 'finance_erp_staging_customer_360_${bundle.target}'" in rendered
    assert "'schema': 'erp'" in rendered
    assert "'root_path': 'src/finance/erp/staging'" in rendered

    spec = PipelineSpec(
        domain="finance",
        source="erp",
        layer="staging",
        asset="customer_360",
    )
    payload = build_pipeline_resource_dict(spec)
    assert payload["catalog"] == "finance"
    assert payload["name"] == "finance_erp_staging_customer_360_${bundle.target}"
    assert payload["schema"] == "erp"


def test_continuous_pipeline_spec_sets_continuous_flag() -> None:
    spec = PipelineSpec(
        domain="template_domain",
        source="template_source",
        layer="staging",
        asset="basic_continuous",
        continuous=True,
        development=True,
        libraries=[{"glob": {"include": "src/template_domain/template_source/staging/**"}}],
    )

    payload = build_pipeline_resource_dict(spec)
    assert payload["continuous"] is True
    assert payload["development"] is True
    assert payload["root_path"] == "src/template_domain/template_source/staging"


def test_full_stack_pipeline_sets_source_root_and_layered_libraries() -> None:
    spec = PipelineSpec(
        domain="finance",
        source="erp",
        layer="full",
        asset="core_streaming",
        pipeline_kind="full_stack",
    )

    payload = build_pipeline_resource_dict(spec)
    assert payload["catalog"] == "finance"
    assert payload["schema"] == "erp"
    assert payload["name"] == "finance_erp_full_core_streaming_${bundle.target}"
    assert payload["root_path"] == "src/finance/erp"
    assert payload["libraries"] == [
        {"glob": {"include": "src/finance/erp/raw/**"}},
        {"glob": {"include": "src/finance/erp/base/**"}},
        {"glob": {"include": "src/finance/erp/staging/**"}},
        {"glob": {"include": "src/finance/erp/final/**"}},
    ]


def test_full_stack_requires_full_layer() -> None:
    with pytest.raises(ValidationError):
        PipelineSpec(
            domain="finance",
            source="erp",
            layer="staging",
            asset="core_streaming",
            pipeline_kind="full_stack",
        )


def test_single_layer_rejects_full_layer_without_full_stack_kind() -> None:
    with pytest.raises(ValidationError):
        PipelineSpec(
            domain="finance",
            source="erp",
            layer="full",
            asset="core_streaming",
            pipeline_kind="single_layer",
        )


def test_full_stack_library_path_must_stay_under_source_root() -> None:
    spec = PipelineSpec(
        domain="finance",
        source="erp",
        layer="full",
        asset="core_streaming",
        pipeline_kind="full_stack",
        libraries=[{"glob": {"include": "src/finance/crm/staging/**"}}],
    )

    with pytest.raises(ValueError):
        build_pipeline_resource_dict(spec)
