.PHONY: install test test-unit test-dq test-integration coverage

install:
	pip install -e ".[dev]"

test:
	pytest -q

test-unit:
	pytest -q -m unit

test-dq:
	pytest -q -m data_quality

test-integration:
	pytest -q -m integration

coverage:
	pytest -q --cov=ecommerce_pipeline --cov-report=term-missing
