.PHONY: sync lint typecheck test ui bench serve

sync:
	uv sync --python 3.13 --all-extras

lint:
	uv run ruff format --check src tests
	uv run ruff check src tests

typecheck:
	uv run mypy src tests

test:
	uv run pytest --cov=helios_consensus --cov-report=term-missing --cov-fail-under=95

ui:
	cd ui && npm ci && npm run build

bench:
	uv run helios simulate-raft
	uv run helios model-check
	uv run helios simulate-hotstuff
	uv run helios benchmark-kernels
	uv run helios eval-ollama

serve:
	uv run helios serve --host 127.0.0.1 --port 8070
