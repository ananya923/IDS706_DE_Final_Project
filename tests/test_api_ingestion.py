import pytest
from unittest.mock import Mock, patch
from etherscan_to_s3_glue import fetch_transactions, build_s3_key, upload_json_to_s3

def test_fetch_transactions_success():
    """Test Etherscan API call returns valid data"""
    with patch('etherscan_to_s3_glue.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = {
            'status': '1',
            'result': [{'hash': '0x123', 'value': '1000'}]
        }
        mock_get.return_value = mock_response
        
        result = fetch_transactions('0xabc', 'test_key')
        assert 'result' in result
        assert len(result['result']) > 0

def test_build_s3_key_format():
    """Test S3 key follows YYYY/MM/DD partition format"""
    key = build_s3_key()
    assert key.startswith('raw/ethereum/txlist/')
    assert '.json' in key
    # Should contain year/month/day format
    parts = key.split('/')
    assert len(parts) >= 5  # prefix/year/month/day/file

def test_api_key_validation():
    """Test that missing API key raises error"""
    with patch.dict('os.environ', {'ETHERSCAN_API_KEY': 'YOUR_ETHERSCAN_API_KEY'}):
        with pytest.raises(RuntimeError):
            from etherscan_to_s3_glue import main
            main()
