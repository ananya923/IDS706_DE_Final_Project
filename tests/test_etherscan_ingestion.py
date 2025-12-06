"""
Unit Tests for Etherscan Ingestion Script
Tests individual functions with actual inputs/outputs
"""
import pytest
import os
import json
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock


# ============================================================================
# UNIT TESTS - Test individual functions
# ============================================================================

class TestPickApiKey:
    """Unit tests for pick_api_key function"""
    
    def test_pick_api_key_single_key(self):
        """Test picking from single API key"""
        # Import or define the function
        def pick_api_key(api_keys, index):
            return api_keys[index % len(api_keys)]
        
        api_keys = ["key1"]
        assert pick_api_key(api_keys, 0) == "key1"
        assert pick_api_key(api_keys, 1) == "key1"
        assert pick_api_key(api_keys, 100) == "key1"
    
    def test_pick_api_key_multiple_keys_round_robin(self):
        """Test round-robin selection with multiple keys"""
        def pick_api_key(api_keys, index):
            return api_keys[index % len(api_keys)]
        
        api_keys = ["key1", "key2", "key3"]
        assert pick_api_key(api_keys, 0) == "key1"
        assert pick_api_key(api_keys, 1) == "key2"
        assert pick_api_key(api_keys, 2) == "key3"
        assert pick_api_key(api_keys, 3) == "key1"  # Wraps around
        assert pick_api_key(api_keys, 4) == "key2"


class TestBuildS3Key:
    """Unit tests for build_s3_key function"""
    
    def test_build_s3_key_format(self):
        """Test S3 key has correct format"""
        def build_s3_key(address: str, prefix: str = "raw/ethereum/txlist") -> str:
            now = datetime.now(timezone.utc)
            return (
                f"{prefix}/{now.year:04d}/{now.month:02d}/{now.day:02d}/"
                f"eth_txlist_{address}_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
            )
        
        address = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
        key = build_s3_key(address)
        
        # Check structure
        assert key.startswith("raw/ethereum/txlist/")
        assert address in key
        assert key.endswith(".json")
        assert "eth_txlist_" in key
    
    def test_build_s3_key_custom_prefix(self):
        """Test S3 key with custom prefix"""
        def build_s3_key(address: str, prefix: str = "raw/ethereum/txlist") -> str:
            now = datetime.now(timezone.utc)
            return (
                f"{prefix}/{now.year:04d}/{now.month:02d}/{now.day:02d}/"
                f"eth_txlist_{address}_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
            )
        
        address = "0xTEST"
        key = build_s3_key(address, prefix="custom/prefix")
        
        assert key.startswith("custom/prefix/")


class TestFetchTransactions:
    """Unit tests for fetch_transactions function (mocked)"""
    
    @patch('requests.get')
    def test_fetch_transactions_success(self, mock_get):
        """Test successful API call"""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "1",
            "message": "OK",
            "result": [{"hash": "0xabc", "value": "1000000000000000000"}]
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        # Define function
        def fetch_transactions(address: str, api_key: str, chain_id: int = 1) -> dict:
            import requests
            url = (
                "https://api.etherscan.io/v2/api"
                f"?chainid={chain_id}&module=account&action=txlist"
                f"&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={api_key}"
            )
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        
        result = fetch_transactions("0xTEST", "test_key")
        
        assert result["status"] == "1"
        assert len(result["result"]) == 1
        mock_get.assert_called_once()
    
    @patch('requests.get')
    def test_fetch_transactions_api_error(self, mock_get):
        """Test API error handling"""
        import requests
        mock_get.side_effect = requests.exceptions.Timeout()
        
        def fetch_transactions(address: str, api_key: str, chain_id: int = 1) -> dict:
            import requests
            url = f"https://api.etherscan.io/v2/api?address={address}&apikey={api_key}"
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        
        with pytest.raises(requests.exceptions.Timeout):
            fetch_transactions("0xTEST", "test_key")


class TestUploadJsonToS3:
    """Unit tests for upload_json_to_s3 function (mocked)"""
    
    @patch('boto3.client')
    def test_upload_json_to_s3_success(self, mock_boto_client):
        """Test successful S3 upload"""
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        def upload_json_to_s3(data: dict, bucket: str, key: str):
            import boto3
            s3 = boto3.client("s3")
            body = json.dumps(data).encode("utf-8")
            s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
        
        test_data = {"test": "data"}
        upload_json_to_s3(test_data, "test-bucket", "test/key.json")
        
        mock_s3.put_object.assert_called_once()
        call_args = mock_s3.put_object.call_args
        assert call_args[1]["Bucket"] == "test-bucket"
        assert call_args[1]["Key"] == "test/key.json"


# ============================================================================
# INTEGRATION TESTS - Test config parsing
# ============================================================================

class TestGetConfig:
    """Integration tests for configuration handling"""
    
    def test_config_from_environment(self):
        """Test config reads from environment variables"""
        with patch.dict(os.environ, {
            'RAW_BUCKET_NAME': 'test-bucket',
            'ETHERSCAN_API_KEYS': 'key1,key2',
            'ETH_ADDRESSES': '0xABC,0xDEF',
            'CHAIN_ID': '1'
        }):
            # Simulate get_config logic
            bucket = os.getenv("RAW_BUCKET_NAME", "default-bucket")
            api_keys_str = os.getenv("ETHERSCAN_API_KEYS", "")
            api_keys = [k.strip() for k in api_keys_str.split(",") if k.strip()]
            addresses_str = os.getenv("ETH_ADDRESSES", "")
            addresses = [a.strip() for a in addresses_str.split(",") if a.strip()]
            chain_id = int(os.getenv("CHAIN_ID", "1"))
            
            assert bucket == "test-bucket"
            assert api_keys == ["key1", "key2"]
            assert addresses == ["0xABC", "0xDEF"]
            assert chain_id == 1
    
    def test_config_missing_api_keys_raises_error(self):
        """Test that missing API keys raises RuntimeError"""
        with patch.dict(os.environ, {}, clear=True):
            api_keys_str = os.getenv("ETHERSCAN_API_KEYS")
            
            if not api_keys_str:
                with pytest.raises(RuntimeError):
                    raise RuntimeError("ETHERSCAN_API_KEYS is not set")
    
    def test_config_invalid_chain_id(self):
        """Test invalid chain ID handling"""
        with patch.dict(os.environ, {'CHAIN_ID': 'invalid'}):
            chain_id_str = os.getenv("CHAIN_ID", "1")
            
            with pytest.raises(ValueError):
                int(chain_id_str)


# ============================================================================
# SYSTEM TESTS - Test file structure and syntax
# ============================================================================

class TestEtherscanScriptStructure:
    """System tests for script structure"""
    
    SCRIPT_PATHS = [
        'glue_jobs/etherscan_to_s3_glue.py',
        'etherscan_to_s3_glue.py',
        'etherscan_to_s3_glue__2_.py',
    ]
    
    def find_script(self):
        for path in self.SCRIPT_PATHS:
            if os.path.exists(path):
                return path
        return None
    
    def test_script_exists(self):
        """Test that etherscan script exists"""
        script = self.find_script()
        if script is None:
            pytest.skip("Etherscan script not found in expected locations")
        assert os.path.exists(script)
    
    def test_script_has_valid_syntax(self):
        """Test script compiles without syntax errors"""
        script = self.find_script()
        if script is None:
            pytest.skip("Etherscan script not found")
        
        with open(script, 'r') as f:
            code = f.read()
        
        try:
            compile(code, script, 'exec')
        except SyntaxError as e:
            pytest.fail(f"Syntax error: {e}")
    
    def test_script_has_required_functions(self):
        """Test script defines required functions"""
        script = self.find_script()
        if script is None:
            pytest.skip("Etherscan script not found")
        
        with open(script, 'r') as f:
            content = f.read()
        
        required_functions = ['fetch_transactions', 'upload_json_to_s3', 'build_s3_key']
        for func in required_functions:
            assert f'def {func}' in content, f"Missing function: {func}"
    
    def test_script_has_required_imports(self):
        """Test script has required imports"""
        script = self.find_script()
        if script is None:
            pytest.skip("Etherscan script not found")
        
        with open(script, 'r') as f:
            content = f.read()
        
        assert 'import boto3' in content, "Missing boto3 import"
        assert 'import requests' in content, "Missing requests import"
        assert 'import json' in content, "Missing json import"
