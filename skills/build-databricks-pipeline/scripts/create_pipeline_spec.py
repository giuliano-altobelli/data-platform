#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path

_LAYER_VALUES = {"raw", "base", "staging", "final"}
_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def _validate_identifier(label: str, value: str) -> str:
    if not _NAME_PATTERN.match(value):
        raise ValueError(f"{label} must match ^[a-z][a-z0-9_]*$: {value}")
    return value


def _bool_groups(parser: argparse.ArgumentParser, name: str) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument(f"--{name}", dest=name, action="store_true")
    group.add_argument(f"--no-{name}", dest=name, action="store_false")
    parser.set_defaults(**{name: None})


def _yaml_bool(value: bool) -> str:
    return "true" if value else "false"


def _parse_config(entries: list[str]) -> dict[str, str]:
    config: dict[str, str] = {}
    for entry in entries:
        if "=" not in entry:
            raise ValueError(f"Invalid --config entry (expected key=value): {entry}")
        key, value = entry.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError(f"Config key must not be empty: {entry}")
        config[key] = value
    return config


def _build_spec_text(
    *,
    domain: str,
    source: str,
    layer: str,
    asset: str,
    continuous: bool | None,
    development: bool | None,
    serverless: bool | None,
    channel: str | None,
    configuration: dict[str, str],
) -> str:
    lines = [
        f"domain: {domain}",
        f"source: {source}",
        f"layer: {layer}",
        f"asset: {asset}",
    ]

    if continuous is not None:
        lines.append(f"continuous: {_yaml_bool(continuous)}")
    if development is not None:
        lines.append(f"development: {_yaml_bool(development)}")
    if serverless is not None:
        lines.append(f"serverless: {_yaml_bool(serverless)}")
    if channel:
        lines.append(f"channel: {channel}")

    if configuration:
        lines.append("configuration:")
        for key in sorted(configuration):
            lines.append(f"  {key}: {configuration[key]}")

    lines.extend(
        [
            "libraries:",
            "  - glob:",
            f"      include: src/{domain}/{source}/{layer}/**",
            "",
        ]
    )
    return "\n".join(lines)


def _resolve_spec_path(spec_path: str | None, domain: str, source: str, asset: str) -> Path:
    if spec_path:
        return Path(spec_path)
    return Path("specs") / "pipelines" / domain / source / f"{asset}.yml"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create/update Databricks pipeline specs.")
    parser.add_argument("--domain", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--layer", required=True, choices=sorted(_LAYER_VALUES))
    parser.add_argument("--asset", required=True)
    parser.add_argument("--spec-path")
    parser.add_argument("--channel")
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--build", action="store_true")
    _bool_groups(parser, "continuous")
    _bool_groups(parser, "development")
    _bool_groups(parser, "serverless")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    try:
        domain = _validate_identifier("domain", args.domain)
        source = _validate_identifier("source", args.source)
        asset = _validate_identifier("asset", args.asset)
        configuration = _parse_config(args.config)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    spec_path = _resolve_spec_path(args.spec_path, domain, source, asset)
    if spec_path.exists() and not args.overwrite:
        print(f"Error: Spec already exists: {spec_path}. Use --overwrite to replace it.")
        return 1

    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_text = _build_spec_text(
        domain=domain,
        source=source,
        layer=args.layer,
        asset=asset,
        continuous=args.continuous,
        development=args.development,
        serverless=args.serverless,
        channel=args.channel,
        configuration=configuration,
    )
    spec_path.write_text(spec_text, encoding="utf-8")
    print(f"Wrote spec: {spec_path}")

    if args.build:
        command = ["just", "build-pipeline-resource", f"spec={spec_path}"]
        result = subprocess.run(command)
        return result.returncode

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
