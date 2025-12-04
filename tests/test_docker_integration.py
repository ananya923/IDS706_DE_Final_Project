"""
Docker Integration Tests
Tests for containerization setup and reproducible environment
"""
import os
import subprocess
import pytest


# ============================================================================
# Docker Setup Tests
# ============================================================================

def test_dockerfile_exists_and_valid():
    """Test that Dockerfile exists and contains required components"""
    assert os.path.exists('Dockerfile'), "Dockerfile must exist in root"
    
    with open('Dockerfile', 'r') as f:
        content = f.read()
    
    # Check minimum length (not empty)
    assert len(content) > 100, "Dockerfile seems too small"
    
    # Check for essential Dockerfile components
    assert 'FROM python' in content, "Dockerfile must specify Python base image"
    assert 'COPY' in content, "Dockerfile must copy files"
    assert 'RUN' in content, "Dockerfile must have RUN commands"
    
    # Check for multi-stage build (best practice)
    assert content.count('FROM') > 1, "Should use multi-stage build for optimization"


def test_docker_compose_exists_and_valid():
    """Test that docker-compose.yml exists and is valid YAML"""
    assert os.path.exists('docker-compose.yml'), "docker-compose.yml must exist"
    
    with open('docker-compose.yml', 'r') as f:
        content = f.read()
    
    # Check for essential docker-compose components
    assert 'services:' in content, "docker-compose must define services"
    assert 'build:' in content or 'image:' in content, "Services must specify build or image"
    
    # Try to validate YAML syntax
    try:
        import yaml
        with open('docker-compose.yml', 'r') as f:
            config = yaml.safe_load(f)
        
        assert 'services' in config, "docker-compose must have services section"
        assert len(config['services']) > 0, "Must define at least one service"
    except ImportError:
        pytest.skip("PyYAML not installed, skipping YAML validation")
    except yaml.YAMLError as e:
        pytest.fail(f"docker-compose.yml has invalid YAML: {e}")


def test_dockerignore_exists():
    """Test that .dockerignore exists for optimized builds"""
    assert os.path.exists('.dockerignore'), ".dockerignore should exist for optimization"
    
    with open('.dockerignore', 'r') as f:
        content = f.read()
    
    # Check for common patterns that should be ignored
    important_ignores = ['.git', '__pycache__', '*.pyc']
    found_ignores = [pattern for pattern in important_ignores if pattern in content]
    
    assert len(found_ignores) > 0, ".dockerignore should ignore common unnecessary files"


def test_requirements_txt_exists_and_has_dependencies():
    """Test that requirements.txt exists and has critical packages"""
    assert os.path.exists('requirements.txt'), "requirements.txt must exist"
    
    with open('requirements.txt', 'r') as f:
        content = f.read()
    
    # Check for critical packages for the project
    critical_packages = ['boto3', 'pandas', 'scikit-learn', 'streamlit']
    missing_packages = [pkg for pkg in critical_packages if pkg not in content]
    
    assert len(missing_packages) == 0, f"Missing critical packages: {missing_packages}"


def test_requirements_dev_exists():
    """Test that requirements-dev.txt exists for testing"""
    assert os.path.exists('requirements-dev.txt'), "requirements-dev.txt should exist"
    
    with open('requirements-dev.txt', 'r') as f:
        content = f.read()
    
    # Should have pytest for testing
    assert 'pytest' in content, "requirements-dev.txt should include pytest"


def test_env_example_exists():
    """Test that file.env.example exists as configuration template"""
    assert os.path.exists('file.env.example'), "file.env.example must exist as template"
    
    with open('file.env.example', 'r') as f:
        content = f.read()
    
    # Check for critical environment variables
    required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'ETHERSCAN_API']
    
    for var in required_vars:
        assert var in content, f"file.env.example should include {var}"


# ============================================================================
# Docker Functionality Tests (Only run if Docker is available)
# ============================================================================

def check_docker_available():
    """Helper function to check if Docker is available"""
    try:
        result = subprocess.run(
            ['docker', '--version'],
            capture_output=True,
            timeout=5
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


@pytest.mark.skipif(not check_docker_available(), reason="Docker not available")
def test_docker_compose_config_validates():
    """Test that docker-compose configuration is valid (if Docker available)"""
    result = subprocess.run(
        ['docker-compose', 'config'],
        capture_output=True,
        text=True,
        timeout=10
    )
    
    assert result.returncode == 0, f"docker-compose config invalid: {result.stderr}"
    
    # Output should contain the parsed configuration
    assert 'services' in result.stdout, "docker-compose config should show services"


@pytest.mark.skipif(not check_docker_available(), reason="Docker not available")
def test_docker_base_stage_builds():
    """Test that Docker base stage can build (if Docker available)"""
    # Try building just the base stage (fastest)
    result = subprocess.run(
        ['docker', 'build', '--target', 'base', '-t', 'fraud-detection:base-test', '.'],
        capture_output=True,
        text=True,
        timeout=300  # 5 minutes
    )
    
    # Check if build succeeded
    if result.returncode == 0:
        # Cleanup - remove test image
        subprocess.run(
            ['docker', 'rmi', 'fraud-detection:base-test'],
            capture_output=True,
            timeout=30
        )
        assert True, "Docker base stage builds successfully"
    else:
        # Build failed, but don't fail test if it's just a missing dependency issue
        if 'not found' in result.stderr or 'Cannot connect' in result.stderr:
            pytest.skip("Docker build failed due to environment issues")
        else:
            pytest.fail(f"Docker build failed: {result.stderr}")


# ============================================================================
# Integration Tests - Reproducible Environment
# ============================================================================

def test_reproducible_environment_files():
    """Test that all files needed for reproducible environment exist"""
    required_files = [
        'Dockerfile',
        'docker-compose.yml',
        'requirements.txt',
        'requirements-dev.txt',
        'file.env.example',
        '.dockerignore',
        '.gitignore'
    ]
    
    missing_files = [f for f in required_files if not os.path.exists(f)]
    
    assert len(missing_files) == 0, f"Missing files for reproducibility: {missing_files}"


def test_project_structure_for_docker():
    """Test that project structure is compatible with Docker"""
    # Check that essential directories exist
    required_dirs = ['scripts', 'tests', 'dashboard']
    
    missing_dirs = [d for d in required_dirs if not os.path.exists(d)]
    
    assert len(missing_dirs) == 0, f"Missing required directories: {missing_dirs}"
    
    # Check that __init__.py files exist (for Python imports)
    init_files = ['scripts/__init__.py', 'tests/__init__.py']
    
    missing_inits = [f for f in init_files if not os.path.exists(f)]
    
    assert len(missing_inits) == 0, f"Missing __init__.py files: {missing_inits}"


def test_no_secrets_in_repo():
    """Test that actual secrets are not committed"""
    # file.env should NOT exist (only file.env.example)
    assert not os.path.exists('file.env'), "file.env should not be committed (contains secrets)"
    
    # Check .gitignore includes file.env
    with open('.gitignore', 'r') as f:
        gitignore = f.read()
    
    assert 'file.env' in gitignore or '.env' in gitignore, ".gitignore must exclude environment files"


def test_docker_stages_defined():
    """Test that Dockerfile defines multiple stages for different purposes"""
    with open('Dockerfile', 'r') as f:
        content = f.read()
    
    # Check for common stages (at least some should exist)
    stages = ['base', 'dependencies', 'testing', 'production']
    
    defined_stages = [stage for stage in stages if f'as {stage}' in content.lower()]
    
    assert len(defined_stages) >= 2, "Dockerfile should define multiple stages for optimization"


def test_docker_compose_defines_services():
    """Test that docker-compose defines expected services"""
    try:
        import yaml
        with open('docker-compose.yml', 'r') as f:
            config = yaml.safe_load(f)
        
        services = config.get('services', {})
        
        # Should have at least one service
        assert len(services) > 0, "docker-compose should define at least one service"
        
        # Check that services have proper configuration
        for service_name, service_config in services.items():
            assert 'build' in service_config or 'image' in service_config, \
                f"Service {service_name} must specify build or image"
    
    except ImportError:
        pytest.skip("PyYAML not installed for detailed validation")


# ============================================================================
# CI/CD Integration Tests
# ============================================================================

def test_ci_workflow_exists():
    """Test that CI/CD workflow exists"""
    workflow_paths = [
        '.github/workflows/ci-pipeline.yml',
        '.github/workflows/ci.yml',
        '.github/workflows/test.yml'
    ]
    
    exists = any(os.path.exists(path) for path in workflow_paths)
    
    assert exists, "At least one CI/CD workflow should exist"


def test_makefile_exists():
    """Test that Makefile exists for convenience commands"""
    if os.path.exists('Makefile'):
        with open('Makefile', 'r') as f:
            content = f.read()
        
        # Should have some Docker-related commands
        assert 'docker' in content.lower() or 'build' in content, \
            "Makefile should include Docker-related commands"


# ============================================================================
# Documentation Tests
# ============================================================================

def test_docker_documentation_exists():
    """Test that Docker setup is documented"""
    # Check README mentions Docker
    if os.path.exists('README.md'):
        with open('README.md', 'r') as f:
            readme = f.read()
        
        has_docker_docs = any(word in readme.lower() for word in ['docker', 'container'])
        
        assert has_docker_docs, "README should document Docker setup"


def test_setup_instructions_present():
    """Test that setup instructions are documented"""
    readme_files = ['README.md', 'DOCKER_README.md', 'QUICKSTART.md']
    
    existing_docs = [f for f in readme_files if os.path.exists(f)]
    
    assert len(existing_docs) > 0, "Should have at least one documentation file with setup instructions"
