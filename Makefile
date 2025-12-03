.PHONY: install test lint deploy-scripts clean

# Install dependencies locally
install:
	pip install --upgrade pip
	pip install -r requirements.txt

# Run all tests
test:
	pytest tests/ -v --tb=short --cov=scripts --cov-report=term-missing

# Run code linting
lint:
	flake8 scripts/ tests/ --max-line-length=100 --exclude=__pycache__

# Deploy Glue scripts to S3
deploy-scripts:
	aws s3 cp scripts/etherscan_to_s3_glue.py s3://de-27-team11/scripts/
	aws s3 cp scripts/glue_feature_engineering.py s3://de-27-team11/scripts/
	aws s3 cp scripts/glue_modeling.py s3://de-27-team11/scripts/
	@echo " Glue scripts deployed to S3"

# Full CI pipeline locally
ci: lint test

# Clean up
clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov/
