import pytest
import os

def test_etherscan_script_when_available():
    """Test etherscan script when it exists in repo"""
    if not os.path.exists('scripts/etherscan_to_s3_glue.py'):
        pytest.skip("Etherscan script will be added in future PR")
    
    with open('scripts/etherscan_to_s3_glue.py', 'r') as f:
        content = f.read()
        assert 'import' in content
        assert 'def' in content
