import pytest
import os

def test_all_scripts_exist():
    """Test that all required scripts are present"""
    required_scripts = [
        'scripts/etherscan_to_s3_glue.py',
        'scripts/glue_feature_engineering.py',
        'scripts/glue_modeling.py'
    ]
    
    for script in required_scripts:
        assert os.path.exists(script), f"Missing required script: {script}"

def test_scripts_are_python_files():
    """Test that scripts are valid Python files"""
    scripts = [
        'scripts/etherscan_to_s3_glue.py',
        'scripts/glue_feature_engineering.py',
        'scripts/glue_modeling.py'
    ]
    
    for script in scripts:
        if os.path.exists(script):
            with open(script, 'r') as f:
                content = f.read()
                # Basic Python syntax check
                assert 'import' in content
                assert 'def ' in content

def test_environment_variables_referenced():
    """Test that scripts reference required environment variables"""
    with open('scripts/etherscan_to_s3_glue.py', 'r') as f:
        content = f.read()
        assert 'BUCKET' in content or 'RAW_BUCKET_NAME' in content
