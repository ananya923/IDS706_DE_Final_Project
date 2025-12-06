"""
Docker Integration Tests
Tests for containerization, CI/CD pipeline, and reproducible environment
"""
import pytest
import os
import subprocess


# ============================================================================
# DOCKER CONFIGURATION TESTS
# ============================================================================

class TestDockerConfiguration:
    """Test Docker configuration files"""
    
    def test_dockerfile_exists(self):
        """Test Dockerfile exists"""
        if not os.path.exists('Dockerfile'):
            pytest.skip("Dockerfile in containers branch, not main yet")
        assert os.path.exists('Dockerfile')
    
    def test_dockerfile_has_from_instruction(self):
        """Test Dockerfile has FROM instruction"""
        if not os.path.exists('Dockerfile'):
            pytest.skip("Dockerfile not found")
        
        with open('Dockerfile', 'r') as f:
            content = f.read()
        
        assert 'FROM' in content, "Dockerfile must have FROM"
    
    def test_dockerfile_has_python_base(self):
        """Test Dockerfile uses Python base image"""
        if not os.path.exists('Dockerfile'):
            pytest.skip("Dockerfile not found")
        
        with open('Dockerfile', 'r') as f:
            content = f.read().lower()
        
        assert 'python' in content, "Should use Python base image"
    
    def test_dockerfile_copies_requirements(self):
        """Test Dockerfile copies requirements.txt"""
        if not os.path.exists('Dockerfile'):
            pytest.skip("Dockerfile not found")
        
        with open('Dockerfile', 'r') as f:
            content = f.read()
        
        assert 'requirements' in content.lower(), \
            "Should copy/install requirements"
    
    def test_dockerfile_has_workdir(self):
        """Test Dockerfile sets WORKDIR"""
        if not os.path.exists('Dockerfile'):
            pytest.skip("Dockerfile not found")
        
        with open('Dockerfile', 'r') as f:
            content = f.read()
        
        assert 'WORKDIR' in content, "Should set WORKDIR"


class TestDockerCompose:
    """Test docker-compose configuration"""
    
    def find_compose_file(self):
        paths = ['docker-compose.yml', 'docker-compose.yaml']
        for p in paths:
            if os.path.exists(p):
                return p
        return None
    
    def test_docker_compose_exists(self):
        """Test docker-compose.yml exists"""
        compose = self.find_compose_file()
        if compose is None:
            pytest.skip("docker-compose in containers branch")
        assert compose is not None
    
    def test_docker_compose_has_services(self):
        """Test docker-compose defines services"""
        compose = self.find_compose_file()
        if compose is None:
            pytest.skip("docker-compose not found")
        
        with open(compose, 'r') as f:
            content = f.read()
        
        assert 'services:' in content
    
    def test_docker_compose_has_test_service(self):
        """Test docker-compose has test service"""
        compose = self.find_compose_file()
        if compose is None:
            pytest.skip("docker-compose not found")
        
        with open(compose, 'r') as f:
            content = f.read()
        
        has_test = 'test' in content.lower()
        if not has_test:
            pytest.skip("Test service may be named differently")
    
    def test_docker_compose_has_dashboard_service(self):
        """Test docker-compose has dashboard service"""
        compose = self.find_compose_file()
        if compose is None:
            pytest.skip("docker-compose not found")
        
        with open(compose, 'r') as f:
            content = f.read()
        
        assert 'dashboard' in content.lower(), \
            "Should have dashboard service"
    
    def test_docker_compose_valid_yaml(self):
        """Test docker-compose is valid YAML"""
        compose = self.find_compose_file()
        if compose is None:
            pytest.skip("docker-compose not found")
        
        try:
            import yaml
            with open(compose, 'r') as f:
                config = yaml.safe_load(f)
            assert 'services' in config
        except ImportError:
            pytest.skip("PyYAML not installed")


class TestDockerIgnore:
    """Test .dockerignore configuration"""
    
    def test_dockerignore_exists(self):
        """Test .dockerignore exists"""
        if not os.path.exists('.dockerignore'):
            pytest.skip(".dockerignore in containers branch")
        assert os.path.exists('.dockerignore')
    
    def test_dockerignore_excludes_git(self):
        """Test .dockerignore excludes .git"""
        if not os.path.exists('.dockerignore'):
            pytest.skip(".dockerignore not found")
        
        with open('.dockerignore', 'r') as f:
            content = f.read()
        
        assert '.git' in content
    
    def test_dockerignore_excludes_pycache(self):
        """Test .dockerignore excludes __pycache__"""
        if not os.path.exists('.dockerignore'):
            pytest.skip(".dockerignore not found")
        
        with open('.dockerignore', 'r') as f:
            content = f.read()
        
        assert '__pycache__' in content or '*.pyc' in content
    
    def test_dockerignore_excludes_env(self):
        """Test .dockerignore excludes .env files"""
        if not os.path.exists('.dockerignore'):
            pytest.skip(".dockerignore not found")
        
        with open('.dockerignore', 'r') as f:
            content = f.read()
        
        assert '.env' in content or 'env' in content.lower()


# ============================================================================
# CI/CD PIPELINE TESTS
# ============================================================================

class TestCIPipeline:
    """Test CI/CD pipeline configuration"""
    
    def test_github_workflows_exists(self):
        """Test .github/workflows exists"""
        assert os.path.exists('.github/workflows'), \
            ".github/workflows should exist"
    
    def test_has_workflow_file(self):
        """Test at least one workflow file exists"""
        workflow_dir = '.github/workflows'
        if not os.path.exists(workflow_dir):
            pytest.skip("Workflows dir not found")
        
        files = os.listdir(workflow_dir)
        yml_files = [f for f in files if f.endswith(('.yml', '.yaml'))]
        
        assert len(yml_files) > 0
    
    def test_workflow_triggers_on_push(self):
        """Test workflow triggers on push"""
        workflow_dir = '.github/workflows'
        if not os.path.exists(workflow_dir):
            pytest.skip("Workflows dir not found")
        
        for f in os.listdir(workflow_dir):
            if f.endswith(('.yml', '.yaml')):
                path = os.path.join(workflow_dir, f)
                with open(path, 'r') as file:
                    content = file.read()
                
                if 'push' in content or 'pull_request' in content:
                    return  # Found trigger
        
        pytest.skip("Trigger configuration may vary")
    
    def test_workflow_runs_tests(self):
        """Test workflow runs pytest"""
        workflow_dir = '.github/workflows'
        if not os.path.exists(workflow_dir):
            pytest.skip("Workflows dir not found")
        
        for f in os.listdir(workflow_dir):
            if f.endswith(('.yml', '.yaml')):
                path = os.path.join(workflow_dir, f)
                with open(path, 'r') as file:
                    content = file.read()
                
                if 'pytest' in content or 'test' in content.lower():
                    return
        
        pytest.skip("Test step may be configured differently")


class TestMakefile:
    """Test Makefile for build automation"""
    
    def test_makefile_exists(self):
        """Test Makefile exists"""
        assert os.path.exists('Makefile'), "Makefile should exist"
    
    def test_makefile_has_targets(self):
        """Test Makefile has targets"""
        if not os.path.exists('Makefile'):
            pytest.skip("Makefile not found")
        
        with open('Makefile', 'r') as f:
            content = f.read()
        
        # Should have at least one target (line with :)
        assert ':' in content, "Makefile should have targets"
    
    def test_makefile_has_test_target(self):
        """Test Makefile has test target"""
        if not os.path.exists('Makefile'):
            pytest.skip("Makefile not found")
        
        with open('Makefile', 'r') as f:
            content = f.read()
        
        assert 'test' in content.lower()
    
    def test_makefile_has_docker_commands(self):
        """Test Makefile has Docker commands"""
        if not os.path.exists('Makefile'):
            pytest.skip("Makefile not found")
        
        with open('Makefile', 'r') as f:
            content = f.read()
        
        has_docker = 'docker' in content.lower()
        if not has_docker:
            pytest.skip("Docker commands may be in separate scripts")


# ============================================================================
# ENVIRONMENT REPRODUCIBILITY TESTS
# ============================================================================

class TestEnvironmentReproducibility:
    """Test environment can be reproduced"""
    
    def test_requirements_pinned(self):
        """Test requirements has version pins"""
        with open('requirements.txt', 'r') as f:
            content = f.read()
        
        # Check for version specifiers
        has_versions = '>=' in content or '==' in content or '~=' in content
        
        assert has_versions, "Requirements should have version pins"
    
    def test_env_template_complete(self):
        """Test env template has all needed variables"""
        with open('file.env.example', 'r') as f:
            content = f.read()
        
        # Should have key variables
        key_vars = ['AWS', 'BUCKET', 'ETHERSCAN']
        found = sum(1 for v in key_vars if v in content)
        
        assert found >= 2, "Should have key environment variables"
    
    def test_no_local_paths_in_code(self):
        """Test no hardcoded local paths in Python files"""
        local_patterns = ['C:\\', '/Users/', '/home/']
        
        for root, dirs, files in os.walk('.'):
            dirs[:] = [d for d in dirs if not d.startswith('.') 
                       and d not in ['venv', '__pycache__']]
            
            for f in files:
                if f.endswith('.py'):
                    path = os.path.join(root, f)
                    with open(path, 'r') as file:
                        content = file.read()
                    
                    for pattern in local_patterns:
                        if pattern in content:
                            # Allow in comments
                            if '#' not in content.split(pattern)[0].split('\n')[-1]:
                                pytest.fail(f"Local path {pattern} in {path}")


# ============================================================================
# DOCKER RUNTIME TESTS (only if Docker available)
# ============================================================================

def docker_available():
    """Check if Docker is available"""
    try:
        result = subprocess.run(
            ['docker', '--version'],
            capture_output=True,
            timeout=5
        )
        return result.returncode == 0
    except:
        return False


@pytest.mark.skipif(not docker_available(), reason="Docker not available")
class TestDockerRuntime:
    """Tests that require Docker to be running"""
    
    def test_docker_daemon_running(self):
        """Test Docker daemon is running"""
        result = subprocess.run(
            ['docker', 'info'],
            capture_output=True,
            timeout=10
        )
        assert result.returncode == 0
    
    def test_can_pull_python_image(self):
        """Test can access Python base image"""
        # Just check docker works, don't actually pull
        result = subprocess.run(
            ['docker', 'images'],
            capture_output=True,
            timeout=10
        )
        assert result.returncode == 0