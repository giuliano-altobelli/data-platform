from __future__ import annotations

from pathlib import Path

from scripts.detect_deployment_scope import detect_scope_from_paths


def test_scope_detection_for_domain_source_changes(tmp_path: Path) -> None:
    changed_paths = [
        "resources/finance/erp/jobs/finance_erp_raw_ingest.py",
        "src/finance/erp/raw/transform.py",
    ]

    result = detect_scope_from_paths(changed_paths, repo_root=tmp_path)

    assert result.full_deploy is False
    assert len(result.scopes) == 1
    assert result.scopes[0].domain == "finance"
    assert result.scopes[0].source == "erp"


def test_global_change_triggers_full_deploy(tmp_path: Path) -> None:
    changed_paths = ["pyproject.toml"]

    result = detect_scope_from_paths(changed_paths, repo_root=tmp_path)

    assert result.full_deploy is True


def test_docs_only_change_has_no_scope_and_no_full_deploy(tmp_path: Path) -> None:
    changed_paths = ["docs/deployment-scope.md"]

    result = detect_scope_from_paths(changed_paths, repo_root=tmp_path)

    assert result.full_deploy is False
    assert result.scopes == ()
