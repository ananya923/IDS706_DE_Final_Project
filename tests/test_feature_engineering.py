import pytest
import os

def test_feature_engineering_script_exists():
    """Test that feature engineering script exists"""
    if not os.path.exists('scripts/glue_feature_engineering.py'):
        pytest.skip("Feature engineering script will be added in data_transformation PR")
    
    assert os.path.exists('scripts/glue_feature_engineering.py')

def test_feature_engineering_has_required_imports():
    """Test that script has required PySpark imports"""
    if not os.path.exists('scripts/glue_feature_engineering.py'):
        pytest.skip("Feature engineering script not yet in repo")
    
    with open('scripts/glue_feature_engineering.py', 'r') as f:
        content = f.read()
        # Check for essential PySpark imports
        assert 'from pyspark.sql import' in content
        assert 'SparkSession' in content
        assert 'from pyspark.sql.functions import' in content

def test_feature_engineering_has_process_function():
    """Test that script has main processing function"""
    if not os.path.exists('scripts/glue_feature_engineering.py'):
        pytest.skip("Feature engineering script not yet in repo")
    
    with open('scripts/glue_feature_engineering.py', 'r') as f:
        content = f.read()
        # Should have a main processing function
        assert 'def process_transactions' in content or 'def main' in content

def test_feature_engineering_has_key_transformations():
    """Test that script includes key feature engineering steps"""
    if not os.path.exists('scripts/glue_feature_engineering.py'):
        pytest.skip("Feature engineering script not yet in repo")
    
    with open('scripts/glue_feature_engineering.py', 'r') as f:
        content = f.read()
        # Check for key transformations mentioned in notebook
        assert 'log_value' in content or 'log1p' in content, "Should have log transformations"
        assert 'datetime' in content or 'timestamp' in content, "Should have temporal features"
        assert 'rolling' in content or 'Window' in content, "Should have rolling/window operations"

def test_feature_engineering_no_hardcoded_paths():
    """Test that script doesn't have hardcoded file paths"""
    if not os.path.exists('scripts/glue_feature_engineering.py'):
        pytest.skip("Feature engineering script not yet in repo")
    
    with open('scripts/glue_feature_engineering.py', 'r') as f:
        content = f.read()
        # Should not have hardcoded local paths
        assert 'C:\\' not in content, "Should not have Windows hardcoded paths"
        assert '/Users/' not in content, "Should not have Mac hardcoded paths"

