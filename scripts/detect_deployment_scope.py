from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections.abc import Iterable
from pathlib import Path

from pydantic import BaseModel, ConfigDict

GLOBAL_PATH_PREFIXES = (
    ".github/workflows/",
    "databricks.yml",
    "justfile",
    "pyproject.toml",
    "resources/_generated/",
    "resources/_models/",
    "scripts/detect_deployment_scope.py",
    "scripts/generate_resource.py",
    "targets/",
)

SCOPE_PATTERNS = (
    re.compile(r"^resources/(?P<domain>[a-z0-9_]+)/(?P<source>[a-z0-9_]+)/"),
    re.compile(r"^src/(?P<domain>[a-z0-9_]+)/(?P<source>[a-z0-9_]+)/"),
    re.compile(r"^specs/(?:jobs|pipelines)/(?P<domain>[a-z0-9_]+)/(?P<source>[a-z0-9_]+)/"),
)

PLACEHOLDER_COMPONENTS = {"template_domain", "template_source"}


class Scope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    domain: str
    source: str


class ScopeDetectionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    full_deploy: bool
    scopes: tuple[Scope, ...]


def _sort_scopes(scopes: Iterable[Scope]) -> tuple[Scope, ...]:
    return tuple(sorted(scopes, key=lambda scope: (scope.domain, scope.source)))


def _normalize_path(path: str) -> str:
    return path.strip().lstrip("./")


def _matches_prefix(path: str, prefix: str) -> bool:
    clean_prefix = prefix.rstrip("/")
    return path == clean_prefix or path.startswith(f"{clean_prefix}/")


def is_global_change(path: str) -> bool:
    return any(_matches_prefix(path, prefix) for prefix in GLOBAL_PATH_PREFIXES)


def _is_placeholder_component(component: str) -> bool:
    return component.startswith("_") or component in PLACEHOLDER_COMPONENTS


def _extract_scope(path: str) -> Scope | None:
    for pattern in SCOPE_PATTERNS:
        match = pattern.match(path)
        if not match:
            continue

        domain = match.group("domain")
        source = match.group("source")
        if _is_placeholder_component(domain) or _is_placeholder_component(source):
            return None
        return Scope(domain=domain, source=source)

    return None


def discover_all_scopes(repo_root: Path) -> tuple[Scope, ...]:
    scope_map: dict[tuple[str, str], Scope] = {}

    for base_dir_name in ("resources", "src"):
        base_dir = repo_root / base_dir_name
        if not base_dir.exists():
            continue

        for domain_dir in base_dir.iterdir():
            if not domain_dir.is_dir() or _is_placeholder_component(domain_dir.name):
                continue

            for source_dir in domain_dir.iterdir():
                if not source_dir.is_dir() or _is_placeholder_component(source_dir.name):
                    continue

                scope = Scope(domain=domain_dir.name, source=source_dir.name)
                scope_map[(scope.domain, scope.source)] = scope

    return _sort_scopes(scope_map.values())


def detect_scope_from_paths(changed_paths: Iterable[str], repo_root: Path) -> ScopeDetectionResult:
    normalized_paths = [_normalize_path(path) for path in changed_paths if path.strip()]
    if not normalized_paths:
        return ScopeDetectionResult(full_deploy=False, scopes=tuple())

    if any(is_global_change(path) for path in normalized_paths):
        scopes = discover_all_scopes(repo_root)
        return ScopeDetectionResult(full_deploy=True, scopes=scopes)

    impacted_scope_map: dict[tuple[str, str], Scope] = {}
    for path in normalized_paths:
        scope = _extract_scope(path)
        if scope is None:
            continue
        impacted_scope_map[(scope.domain, scope.source)] = scope

    return ScopeDetectionResult(full_deploy=False, scopes=_sort_scopes(impacted_scope_map.values()))


def _git_diff_files(base: str, head: str) -> list[str]:
    if not base or base == "0" * 40:
        command = ["git", "diff", "--name-only", f"{head}~1", head]
    else:
        command = ["git", "diff", "--name-only", base, head]

    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _write_github_output(path: Path, result: ScopeDetectionResult) -> None:
    scopes_payload = json.dumps([scope.model_dump() for scope in result.scopes])
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"full_deploy={'true' if result.full_deploy else 'false'}\n")
        handle.write(f"has_scopes={'true' if bool(result.scopes) else 'false'}\n")
        handle.write(f"scopes={scopes_payload}\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Detect deployment scope from git diff.")
    parser.add_argument("--base", type=str, default="")
    parser.add_argument("--head", type=str, default="HEAD")
    parser.add_argument("--changed-file", action="append", default=[])
    parser.add_argument("--github-output", type=Path)

    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[1]

    changed_files = args.changed_file
    if not changed_files:
        changed_files = _git_diff_files(args.base, args.head)

    result = detect_scope_from_paths(changed_files, repo_root)
    payload = {
        "full_deploy": result.full_deploy,
        "scopes": [scope.model_dump() for scope in result.scopes],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))

    if args.github_output is not None:
        _write_github_output(args.github_output, result)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
