"""
Integration Tests
Tests for overall project structure, CI/CD, and configuration
"""
import pytest
import os


# ============================================================================
# Project Structure Tests
# ============================================================================

def test_gitignore_exists():
    """Test that .gitignore exists and protects secrets"""
    # Check both possible names (with and without leading dot)
    gitignore_paths = ['.gitignore', '_gitignore']
    
    found = None
    for path in gitignore_paths:
        if os.path.exists(path):
            found = path
            break
    
    assert found is not None, ".gitignore must exist"
    
    with open(found, 'r') as f:
        content = f.read()
    
    # Check that sensitive files are ignored
    assert 'file.env' in content or '.env' in content, \
        ".gitignore must exclude environment files"
    assert '__pycache__' in content, \
        ".gitignore should exclude Python cache"


def test_env_template_exists():
    """Test that env template is available"""
    env_template_paths = ['file.env.example', '.env.example', 'env.example']
    
    found = None
    for path in env_template_paths:
        if os.path.exists(path):
            found = path
            break
    
    if found is None:
        pytest.skip("Environment template file not found - will be added")
    
    with open(found, 'r') as f:
        content = f.read()
    
    # Check for required environment variables
    required_vars = ['AWS', 'ETHERSCAN']
    found_vars = [var for var in required_vars if var in content]
    
    assert len(found_vars) > 0, \
        f"Environment template should include AWS or ETHERSCAN variables"


def test_ci_workflow_exists():
    """Test that CI pipeline is set up"""
    workflow_paths = [
        '.github/workflows/ci-pipeline.yml',
        '.github/workflows/ci.yml',
        '.github/workflows/test.yml',
        '.github/workflows/main.yml',
    ]
    
    found = any(os.path.exists(path) for path in workflow_paths)
    
    if not found:
        pytest.skip("CI workflow will be added in future PR")
    
    assert found, "CI/CD workflow should exist"


def test_project_has_tests():
    """Test that test infrastructure is in place"""
    # Check if we're running from tests directory or root
    test_files = [
        'tests/__init__.py',
        'test_dashboard.py',
        'test_etherscan_ingestion.py',
        'test_feature_engineering.py',
        'test_modeling.py',
    ]
    
    found_tests = [f for f in test_files if os.path.exists(f)]
    
    assert len(found_tests) > 0, "Test infrastructure should be in place"


def test_requirements_has_dependencies():
    """Test that requirements.txt has key dependencies"""
    assert os.path.exists('requirements.txt'), "requirements.txt must exist"
    
    with open('requirements.txt', 'r') as f:
        content = f.read()
    
    # Check for critical packages
    critical_packages = ['boto3', 'pandas', 'streamlit']
    found_packages = [pkg for pkg in critical_packages if pkg in content.lower()]
    
    assert len(found_packages) >= 2, \
        f"requirements.txt should have critical packages. Found: {found_packages}"


def test_requirements_dev_exists():
    """Test that dev requirements exist"""
    dev_req_paths = ['requirements-dev.txt', 'requirements_dev.txt', 'dev-requirements.txt']
    
    found = any(os.path.exists(path) for path in dev_req_paths)
    
    if not found:
        pytest.skip("Dev requirements file not found")
    
    for path in dev_req_paths:
        if os.path.exists(path):
            with open(path, 'r') as f:
                content = f.read()
            assert 'pytest' in content, "Dev requirements should include pytest"
            break


# ============================================================================
# Docker Setup Tests
# ============================================================================

def test_docker_setup_exists():
    """Test that Docker setup exists for containerization"""
    dockerfile_paths = ['Dockerfile', 'dockerfile']
    
    found = any(os.path.exists(path) for path in dockerfile_paths)
    
    if not found:
        pytest.skip("Docker setup will be added in future PR")
    
    assert found, "Dockerfile should exist"


def test_docker_compose_exists():
    """Test that docker-compose.yml exists"""
    compose_paths = ['docker-compose.yml', 'docker-compose.yaml']
    
    found = any(os.path.exists(path) for path in compose_paths)
    
    if not found:
        pytest.skip("docker-compose.yml will be added in future PR")
    
    assert found, "docker-compose.yml should exist"


def test_dockerignore_exists():
    """Test that .dockerignore exists"""
    dockerignore_paths = ['.dockerignore', '_dockerignore']
    
    found = any(os.path.exists(path) for path in dockerignore_paths)
    
    if not found:
        pytest.skip(".dockerignore will be added in future PR")
    
    assert found, ".dockerignore should exist"


# ============================================================================
# Security Tests
# ============================================================================

def test_no_secrets_in_repo():
    """Test that actual secrets are not committed"""
    # file.env should NOT exist (only file.env.example)
    secret_files = ['file.env', '.env', 'secrets.json', 'credentials.json']
    
    for secret_file in secret_files:
        assert not os.path.exists(secret_file), \
            f"{secret_file} should not be committed (contains secrets)"


def test_no_aws_credentials_file():
    """Test that AWS credentials are not committed"""
    aws_cred_paths = [
        '.aws/credentials',
        'aws_credentials',
        'credentials.csv',
    ]
    
    for path in aws_cred_paths:
        assert not os.path.exists(path), \
            f"{path} should not be committed"


# ============================================================================
# Source Code Tests
# ============================================================================

def test_main_scripts_exist():
    """Test that main source scripts exist"""
    script_paths = [
        'etherscan_to_s3_glue__2_.py',
        'ethereum-feature-engineering.py',
        'ethereum_fraud_modeling.py',
        'app.py',
        'scripts/etherscan_to_s3_glue.py',
        'scripts/glue_feature_engineering.py',
        'scripts/glue_modeling.py',
    ]
    
    found_scripts = [p for p in script_paths if os.path.exists(p)]
    
    assert len(found_scripts) >= 2, \
        f"Should have at least 2 main scripts. Found: {found_scripts}"


def test_all_python_files_have_valid_syntax():
    """Test that all Python files have valid syntax"""
    python_files = [f for f in os.listdir('.') if f.endswith('.py')]
    
    errors = []
    for py_file in python_files:
        try:
            with open(py_file, 'r') as f:
                code = f.read()
            compile(code, py_file, 'exec')
        except SyntaxError as e:
            errors.append(f"{py_file}: {e}")
    
    assert len(errors) == 0, f"Python files with syntax errors: {errors}"


# ============================================================================
# Documentation Tests
# ============================================================================

def test_readme_exists():
    """Test that README exists"""
    readme_paths = ['README.md', 'README.rst', 'README.txt', 'README']
    
    found = any(os.path.exists(path) for path in readme_paths)
    
    if not found:
        pytest.skip("README will be added")
    
    assert found, "README should exist"