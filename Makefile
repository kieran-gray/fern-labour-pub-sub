SRC_DIR=src
TEST_DIR=tests
PYPROJECT_TOML=pyproject.toml

# Source code formatting and linting
.PHONY: format \
		lint

format:
	uv run ruff format $(SRC_DIR) $(TEST_DIR)
	uv run ruff check $(SRC_DIR) $(TEST_DIR) --fix

lint:
	uv lock --check
	uv run mypy $(SRC_DIR) $(TEST_DIR)
	uv run ruff check $(SRC_DIR) $(TEST_DIR)
	uv run ruff format $(SRC_DIR) $(TEST_DIR) --check
	uv run bandit -r $(SRC_DIR) $(TEST_DIR) -c $(PYPROJECT_TOML)

.PHONY: test \
		test-debug \
		smoke-test \
		ci-smoke-tests

test:
	uv run pytest --cov

test-debug:
	uv run debugpy --listen 0.0.0.0:5678 --wait-for-client -m pytest $(TEST_DIR) -v

smoke-test:
	uv run $(TEST_DIR)/smoke_test.py

ci-smoke-tests:
	@echo "Running smoke tests on wheel"
	uv run --isolated --no-project -p 3.12 --with dist/*.whl tests/smoke_test.py
	@echo "Running smoke tests on source distribution"
	uv run --isolated --no-project -p 3.12 --with dist/*.tar.gz tests/smoke_test.py