set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

build-job-resource spec:
    spec_path="{{spec}}"; spec_path="${spec_path#spec=}"; uv run python scripts/generate_resource.py --kind job --spec "$spec_path"

build-pipeline-resource spec:
    spec_path="{{spec}}"; spec_path="${spec_path#spec=}"; uv run python scripts/generate_resource.py --kind pipeline --spec "$spec_path"

format:
    uv run ruff format .

lint:
    uv run ruff check .

lint-fix:
    uv run ruff check --fix .

typecheck:
    uv run pyrefly check --summarize-errors

check:
    uv run ruff format --check .
    uv run ruff check .
    uv run pyrefly check --summarize-errors
