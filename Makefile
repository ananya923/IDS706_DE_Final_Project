.PHONY: help build test run-tests clean push ingestion features modeling dashboard pipeline lint format

# Default target
help:
	@echo "Ethereum Fraud Detection - Docker Commands"
	@echo ""
	@echo "Build Commands:"
	@echo "  make build              - Build all Docker images"
	@echo "  make build-test         - Build testing image"
	@echo "  make build-ingestion    - Build ingestion image"
	@echo "  make build-features     - Build feature engineering image"
	@echo "  make build-modeling     - Build modeling image"
	@echo "  make build-dashboard    - Build dashboard image"
	@echo ""
	@echo "Run Commands:"
	@echo "  make test               - Run all tests"
	@echo "  make ingestion          - Run data ingestion"
	@echo "  make features           - Run feature engineering"
	@echo "  make modeling           - Run model training"
	@echo "  make dashboard          - Start Streamlit dashboard (http://localhost:8501)"
	@echo "  make pipeline           - Run complete pipeline"
	@echo ""
	@echo "Development Commands:"
	@echo "  make lint               - Run code quality checks"
	@echo "  make format             - Format code with black"
	@echo "  make clean              - Remove containers and images"
	@echo "  make logs SERVICE=name  - View logs for a service"
	@echo ""
	@echo "Docker Commands:"
	@echo "  make up                 - Start all services"
	@echo "  make down               - Stop all services"
	@echo "  make ps                 - List running containers"

# ============================================================================
# Build Targets
# ============================================================================

build:
	@echo "Building all Docker images..."
	docker-compose build

build-test:
	@echo "Building test image..."
	docker-compose build test

build-ingestion:
	@echo "Building ingestion image..."
	docker-compose build ingestion

build-features:
	@echo "Building feature engineering image..."
	docker-compose build feature-engineering

build-modeling:
	@echo "Building modeling image..."
	docker-compose build modeling

build-dashboard:
	@echo "Building dashboard image..."
	docker-compose build dashboard

# ============================================================================
# Run Targets
# ============================================================================

test: build-test
	@echo "Running tests..."
	docker-compose run --rm test

ingestion: build-ingestion
	@echo "Running data ingestion..."
	docker-compose --profile ingestion run --rm ingestion

features: build-features
	@echo "Running feature engineering..."
	docker-compose --profile feature-engineering run --rm feature-engineering

modeling: build-modeling
	@echo "Running model training..."
	docker-compose --profile modeling run --rm modeling

dashboard: build-dashboard
	@echo "Starting Streamlit dashboard..."
	@echo "Access at: http://localhost:8501"
	docker-compose --profile dashboard up dashboard

pipeline: build
	@echo "Running complete pipeline..."
	docker-compose --profile pipeline run --rm pipeline

# ============================================================================
# Development Targets
# ============================================================================

lint:
	@echo "Running linters..."
	docker-compose run --rm test flake8 scripts/ tests/
	docker-compose run --rm test pylint scripts/ tests/ --disable=C0111,R0913

format:
	@echo "Formatting code with black..."
	docker-compose run --rm test black scripts/ tests/

clean:
	@echo "Cleaning up containers and images..."
	docker-compose down -v
	docker system prune -f

logs:
	@if [ -z "$(SERVICE)" ]; then \
		echo "Usage: make logs SERVICE=<service-name>"; \
		echo "Available services: test, ingestion, feature-engineering, modeling, dashboard"; \
	else \
		docker-compose logs -f $(SERVICE); \
	fi

# ============================================================================
# Docker Compose Helpers
# ============================================================================

up:
	docker-compose --profile dashboard up -d

down:
	docker-compose down

ps:
	docker-compose ps

restart:
	docker-compose restart

# ============================================================================
# CI/CD Helpers
# ============================================================================

ci-test: build-test
	@echo "Running CI tests..."
	docker-compose run --rm test pytest tests/ -v --tb=short --junitxml=test-results.xml

ci-build:
	@echo "Building for CI..."
	docker build --target production -t fraud-detection:latest .

# ============================================================================
# AWS ECR Push (for production deployment)
# ============================================================================

ECR_REPO ?= your-account-id.dkr.ecr.us-east-1.amazonaws.com/fraud-detection
VERSION ?= latest

ecr-login:
	@echo "Logging into AWS ECR..."
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $(ECR_REPO)

push: ecr-login
	@echo "Tagging and pushing images to ECR..."
	docker tag fraud-detection:latest $(ECR_REPO):$(VERSION)
	docker push $(ECR_REPO):$(VERSION)

push-all: ecr-login
	@echo "Pushing all service images..."
	docker-compose build
	docker tag fraud-detection-ingestion:latest $(ECR_REPO)/ingestion:$(VERSION)
	docker tag fraud-detection-features:latest $(ECR_REPO)/features:$(VERSION)
	docker tag fraud-detection-modeling:latest $(ECR_REPO)/modeling:$(VERSION)
	docker tag fraud-detection-dashboard:latest $(ECR_REPO)/dashboard:$(VERSION)
	docker push $(ECR_REPO)/ingestion:$(VERSION)
	docker push $(ECR_REPO)/features:$(VERSION)
	docker push $(ECR_REPO)/modeling:$(VERSION)
	docker push $(ECR_REPO)/dashboard:$(VERSION)

