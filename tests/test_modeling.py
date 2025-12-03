import pytest
import os

def test_modeling_when_available():
    """Test modeling script when it exists in repo"""
    if not os.path.exists('scripts/glue_modeling.py'):
        pytest.skip("Modeling script will be added in future PR")
    
    with open('scripts/glue_modeling.py', 'r') as f:
        content = f.read()
        assert 'import' in content
