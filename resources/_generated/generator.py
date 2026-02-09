from __future__ import annotations

import argparse
import pprint
import textwrap
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import ValidationError

from resources._models.specs import JobSpec, PipelineSpec

ResourceKind = Literal["job", "pipeline"]
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RESOURCE_ROOT = REPO_ROOT / "resources"


def _read_spec_file(spec_path: Path) -> dict[str, Any]:
    if not spec_path.exists():
        raise FileNotFoundError(f"Spec file does not exist: {spec_path}")

    raw = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Spec file must contain a top-level mapping.")
    return raw


def _merge_tags(
    custom_tags: dict[str, str], domain: str, source: str, layer: str
) -> dict[str, str]:
    merged = {
        "domain": domain,
        "layer": layer,
        "source": source,
    }
    merged.update(custom_tags)
    return merged


def _with_bundle_target_suffix(base_name: str) -> str:
    return f"{base_name}_${{bundle.target}}"


def _validate_reserved_parameters(parameters: list[dict[str, Any]]) -> None:
    for parameter in parameters:
        if not isinstance(parameter, dict):
            raise ValueError("Every job parameter must be a mapping.")

        if parameter.get("name") in {"catalog", "schema"}:
            raise ValueError("Job parameters named 'catalog' and 'schema' are reserved.")


def build_job_resource_dict(spec: JobSpec) -> dict[str, Any]:
    _validate_reserved_parameters(spec.parameters)

    resource: dict[str, Any] = {
        "name": _with_bundle_target_suffix(spec.resource_name),
        "parameters": [
            {"name": "catalog", "default": spec.domain},
            {"name": "schema", "default": spec.source},
            *spec.parameters,
        ],
        "tags": _merge_tags(spec.tags, spec.domain, spec.source, spec.layer),
        "tasks": spec.tasks,
    }

    if spec.description is not None:
        resource["description"] = spec.description
    if spec.max_concurrent_runs is not None:
        resource["max_concurrent_runs"] = spec.max_concurrent_runs
    if spec.trigger is not None:
        resource["trigger"] = spec.trigger
    if spec.schedule is not None:
        resource["schedule"] = spec.schedule
    if spec.email_notifications is not None:
        resource["email_notifications"] = spec.email_notifications
    if spec.timeout_seconds is not None:
        resource["timeout_seconds"] = spec.timeout_seconds
    if spec.job_clusters:
        resource["job_clusters"] = spec.job_clusters

    if spec.environments:
        resource["environments"] = spec.environments
    else:
        resource["environments"] = [
            {
                "environment_key": "default",
                "spec": {
                    "dependencies": ["dist/*.whl"],
                    "environment_version": "4",
                },
            }
        ]

    return resource


def _path_from_library_value(key: str, value: Any) -> str:
    if key == "glob":
        if not isinstance(value, dict) or not isinstance(value.get("include"), str):
            raise ValueError("A glob library must be shaped as {'glob': {'include': '<path>'}}.")
        return value["include"]

    if isinstance(value, str):
        return value

    if isinstance(value, dict) and isinstance(value.get("path"), str):
        return value["path"]

    raise ValueError(f"Library '{key}' must be a path string or a mapping with 'path'.")


def _build_pipeline_paths(spec: PipelineSpec) -> tuple[str, str, str, str]:
    source_root = f"src/{spec.domain}/{spec.source}"
    layer_path = f"{source_root}/{spec.layer}"

    if spec.pipeline_kind == "full_stack":
        root_path = source_root
        allowed_library_root = source_root
    else:
        root_path = layer_path
        allowed_library_root = layer_path

    return source_root, layer_path, root_path, allowed_library_root


def _default_pipeline_libraries(
    spec: PipelineSpec, source_root: str, layer_path: str
) -> list[dict[str, Any]]:
    if spec.pipeline_kind == "full_stack":
        return [
            {"glob": {"include": f"{source_root}/raw/**"}},
            {"glob": {"include": f"{source_root}/base/**"}},
            {"glob": {"include": f"{source_root}/staging/**"}},
            {"glob": {"include": f"{source_root}/final/**"}},
        ]
    return [{"glob": {"include": f"{layer_path}/**"}}]


def _normalize_pipeline_libraries(
    spec: PipelineSpec, source_root: str, layer_path: str, allowed_library_root: str
) -> list[dict[str, Any]]:
    if not spec.libraries:
        return _default_pipeline_libraries(spec, source_root=source_root, layer_path=layer_path)

    supported_keys = {"glob", "file", "notebook"}
    normalized: list[dict[str, Any]] = []

    for library in spec.libraries:
        if not isinstance(library, dict):
            raise ValueError("Each pipeline library must be a mapping.")

        library_keys = set(library)
        if len(library_keys) != 1 or not library_keys.issubset(supported_keys):
            raise ValueError(
                "Each pipeline library must contain exactly one of: glob, file, notebook."
            )

        key = next(iter(library_keys))
        value = library[key]
        candidate_path = _path_from_library_value(key, value)
        if not candidate_path.startswith(f"{allowed_library_root}/"):
            if spec.pipeline_kind == "full_stack":
                raise ValueError(
                    "Pipeline library path must align to source folder "
                    f"'{source_root}/'. Found: {candidate_path}"
                )
            raise ValueError(
                "Pipeline library path must align to layer folder "
                f"'{layer_path}/'. Found: {candidate_path}"
            )

        normalized.append(library)

    return normalized


def build_pipeline_resource_dict(spec: PipelineSpec) -> dict[str, Any]:
    source_root, layer_path, root_path, allowed_library_root = _build_pipeline_paths(spec)

    resource: dict[str, Any] = {
        "catalog": spec.domain,
        "libraries": _normalize_pipeline_libraries(
            spec,
            source_root=source_root,
            layer_path=layer_path,
            allowed_library_root=allowed_library_root,
        ),
        "name": _with_bundle_target_suffix(spec.resource_name),
        "root_path": root_path,
        "schema": spec.source,
        "serverless": spec.serverless,
        "tags": _merge_tags(spec.tags, spec.domain, spec.source, spec.layer),
    }

    if spec.environment is not None:
        resource["environment"] = spec.environment
    else:
        resource["environment"] = {"dependencies": ["--editable ${workspace.file_path}"]}

    if spec.development is not None:
        resource["development"] = spec.development
    if spec.continuous is not None:
        resource["continuous"] = spec.continuous
    if spec.channel is not None:
        resource["channel"] = spec.channel
    if spec.edition is not None:
        resource["edition"] = spec.edition
    if spec.photon is not None:
        resource["photon"] = spec.photon
    if spec.clusters:
        resource["clusters"] = spec.clusters
    if spec.configuration:
        resource["configuration"] = spec.configuration

    return resource


def _render_module(
    *,
    import_path: str,
    resource_class: str,
    variable_name: str,
    resource_payload: dict[str, Any],
) -> str:
    formatted_payload = pprint.pformat(resource_payload, indent=4, sort_dicts=True, width=88)
    indented_payload = textwrap.indent(formatted_payload, " " * 4)

    return (
        f"from {import_path} import {resource_class}\n\n"
        "# Generated by scripts/generate_resource.py. Do not edit manually.\n"
        f"{variable_name} = {resource_class}.from_dict(\n"
        f"{indented_payload}\n"
        ")\n"
    )


def render_job_module(spec: JobSpec) -> str:
    return _render_module(
        import_path="databricks.bundles.jobs",
        resource_class="Job",
        variable_name=spec.resource_name,
        resource_payload=build_job_resource_dict(spec),
    )


def render_pipeline_module(spec: PipelineSpec) -> str:
    return _render_module(
        import_path="databricks.bundles.pipelines",
        resource_class="Pipeline",
        variable_name=spec.resource_name,
        resource_payload=build_pipeline_resource_dict(spec),
    )


def _ensure_package_tree(resource_root: Path, package_dir: Path) -> None:
    resource_root.mkdir(parents=True, exist_ok=True)
    if not (resource_root / "__init__.py").exists():
        (resource_root / "__init__.py").write_text("\n", encoding="utf-8")

    relative_parts = package_dir.relative_to(resource_root).parts
    current = resource_root
    for part in relative_parts:
        current = current / part
        current.mkdir(parents=True, exist_ok=True)
        init_path = current / "__init__.py"
        if not init_path.exists():
            init_path.write_text("\n", encoding="utf-8")


def build_job_resource(spec_path: Path, resource_root: Path | None = None) -> Path:
    raw_spec = _read_spec_file(spec_path)
    spec = JobSpec.model_validate(raw_spec)

    destination_root = resource_root or DEFAULT_RESOURCE_ROOT
    package_dir = destination_root / spec.domain / spec.source / "jobs"
    _ensure_package_tree(destination_root, package_dir)

    output_path = package_dir / f"{spec.resource_name}.py"
    output_path.write_text(render_job_module(spec), encoding="utf-8")
    return output_path


def build_pipeline_resource(spec_path: Path, resource_root: Path | None = None) -> Path:
    raw_spec = _read_spec_file(spec_path)
    spec = PipelineSpec.model_validate(raw_spec)

    destination_root = resource_root or DEFAULT_RESOURCE_ROOT
    package_dir = destination_root / spec.domain / spec.source / "pipelines"
    _ensure_package_tree(destination_root, package_dir)

    output_path = package_dir / f"{spec.resource_name}.py"
    output_path.write_text(render_pipeline_module(spec), encoding="utf-8")
    return output_path


def build_resource(kind: ResourceKind, spec_path: Path, resource_root: Path | None = None) -> Path:
    if kind == "job":
        return build_job_resource(spec_path, resource_root=resource_root)
    return build_pipeline_resource(spec_path, resource_root=resource_root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate Databricks resources from strict specs.")
    parser.add_argument("--kind", choices=("job", "pipeline"), required=True)
    parser.add_argument("--spec", required=True, type=Path)

    args = parser.parse_args(argv)

    try:
        output_path = build_resource(args.kind, args.spec)
    except (FileNotFoundError, ValidationError, ValueError) as exc:
        print(f"Error: {exc}")
        return 1

    print(f"Generated: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
