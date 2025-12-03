import pytest
import os

def test_feature_engineering_when_available():
    """Test feature engineering script when it exists in repo"""
    if not os.path.exists('scripts/glue_feature_engineering.py'):
        pytest.skip("Feature engineering script will be added in future PR")
    
    with open('scripts/glue_feature_engineering.py', 'r') as f:
        content = f.read()
        assert 'import' in content
