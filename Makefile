.PHONY: install test lint deploy-scripts deploy-dashboard clean

# Install dependencies locally
install:
	pip install --upgrade pip
	pip install -r requirements.txt

# Run all tests
test:
	pytest tests/ -v --tb=short

# Run code linting
lint:
	flake8 scripts/ tests/

# Deploy Glue scripts to S3
deploy-scripts:
	aws s3 cp scripts/etherscan_to_s3_glue.py s3://de-27-team11/scripts/
	aws s3 cp scripts/glue_feature_engineering.py s3://de-27-team11/scripts/
	aws s3 cp scripts/glue_modeling.py s3://de-27-team11/scripts/
	@echo "Glue scripts deployed to S3"

# Deploy CloudFormation stack
deploy-infrastructure:
	aws cloudformation deploy \
		--template-file temp_ingestion_infrastructure.yaml \
		--stack-name ethereum-fraud-detection \
		--capabilities CAPABILITY_NAMED_IAM \
		--parameter-overrides EtherscanApiKey=${ETHERSCAN_API_KEY}

# Build and push dashboard Docker image
deploy-dashboard:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ${ECR_REGISTRY}
	docker build -f Dockerfile.dashboard -t ethereum-fraud-dashboard .
	docker tag ethereum-fraud-dashboard:latest ${ECR_REGISTRY}/ethereum-fraud-dashboard:latest
	docker push ${ECR_REGISTRY}/ethereum-fraud-dashboard:latest

# Run tests locally with Docker
test-local:
	docker-compose up -d mysql
	sleep 10
	pytest tests/ -v --tb=short
	docker-compose down

# Full CI pipeline locally
ci: lint test

# Clean up
clean:
	docker-compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
