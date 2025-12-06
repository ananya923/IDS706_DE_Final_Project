"""
Integration and System Tests
Tests for project structure, CI/CD, and Docker setup
"""
import pytest
import os


# ============================================================================
# PROJECT STRUCTURE TESTS
# ============================================================================

def test_gitignore_exists():
    """Test that .gitignore exists and protects secrets"""
    assert os.path.exists('.gitignore'), ".gitignore must exist"
    
    with open('.gitignore', 'r') as f:
        content = f.read()
    
    assert '.env' in content or 'file.env' in content, \
        ".gitignore must exclude environment files"


def test_env_template_exists():
    """Test that file.env.example exists"""
    assert os.path.exists('file.env.example'), "file.env.example must exist"


def test_ci_workflow_exists():
    """Test that CI pipeline is set up"""
    assert os.path.exists('.github/workflows'), \
        ".github/workflows folder should exist"


def test_project_has_tests():
    """Test that test files exist"""
    assert os.path.exists('tests'), "tests folder should exist"


def test_requirements_has_dependencies():
    """Test that requirements.txt has key dependencies"""
    assert os.path.exists('requirements.txt'), "requirements.txt must exist"
    
    with open('requirements.txt', 'r') as f:
        content = f.read().lower()
    
    assert 'boto3' in content or 'pandas' in content or 'streamlit' in content


def test_requirements_dev_exists():
    """Test that dev requirements exist"""
    paths = ['requirements-dev.txt', 'requirements_dev.txt']
    exists = any(os.path.exists(p) for p in paths)
    
    if not exists:
        pytest.skip("Dev requirements file not found")


def test_docker_setup_exists():
    """Test that Dockerfile exists"""
    assert os.path.exists('Dockerfile'), "Dockerfile should exist"


def test_docker_compose_exists():
    """Test that docker-compose.yml exists"""
    paths = ['docker-compose.yml', 'docker-compose.yaml']
    exists = any(os.path.exists(p) for p in paths)
    assert exists, "docker-compose.yml should exist"


def test_dockerignore_exists():
    """Test that .dockerignore exists"""
    assert os.path.exists('.dockerignore'), ".dockerignore should exist"


def test_no_secrets_in_repo():
    """Test that actual secrets are not committed"""
    secret_files = ['file.env', '.env', 'secrets.json']
    committed = [f for f in secret_files if os.path.exists(f)]
    assert len(committed) == 0, f"Secret files should not be committed: {committed}"


def test_no_aws_credentials_file():
    """Test that AWS credentials are not committed"""
    paths = ['.aws/credentials', 'credentials.csv']
    committed = [f for f in paths if os.path.exists(f)]
    assert len(committed) == 0, f"AWS credentials should not be committed"


def test_glue_jobs_folder_exists():
    """Test that glue_jobs folder exists"""
    assert os.path.exists('glue_jobs'), "glue_jobs folder should exist"


def test_dashboard_folder_exists():
    """Test that dashboard folder exists"""
    assert os.path.exists('dashboard'), "dashboard folder should exist"


def test_all_python_files_have_valid_syntax():
    """Test that all Python files have valid syntax"""
    errors = []
    
    dirs[:] = [d for d in dirs if not d.startswith('.') 
           and d not in ['venv', '__pycache__', 'scripts']]
        
        for f in files:
            if f.endswith('.py'):
                path = os.path.join(root, f)
                try:
                    with open(path, 'r') as file:
                        code = file.read()
                    compile(code, path, 'exec')
                except SyntaxError as e:
                    errors.append(f"{path}: {e}")
    
    assert len(errors) == 0, f"Syntax errors: {errors}"


def test_readme_exists():
    """Test README documentation exists"""
    assert os.path.exists('README.md'), "README.md should exist"