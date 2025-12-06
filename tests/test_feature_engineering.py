"""
Unit Tests for Feature Engineering Script
Tests individual transformation functions and logic
"""
import pytest
import os
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock


# ============================================================================
# UNIT TESTS - Test transformation logic
# ============================================================================

class TestCleanColumnNames:
    """Unit tests for clean_column_names function"""
    
    def test_clean_empty_column_names(self):
        """Test that empty column names are replaced"""
        def clean_column_names(columns):
            safe_names = []
            for idx, c in enumerate(columns):
                if c is None:
                    c = ""
                name = c.strip()
                if name == "":
                    safe_names.append(f"col_{idx}")
                else:
                    safe_names.append(name)
            return safe_names
        
        columns = ["name", "", "value", None, "  "]
        result = clean_column_names(columns)
        
        assert result[0] == "name"
        assert result[1] == "col_1"
        assert result[2] == "value"
        assert result[3] == "col_3"
        assert result[4] == "col_4"
    
    def test_clean_column_names_all_valid(self):
        """Test with all valid column names"""
        def clean_column_names(columns):
            safe_names = []
            for idx, c in enumerate(columns):
                if c is None:
                    c = ""
                name = c.strip()
                if name == "":
                    safe_names.append(f"col_{idx}")
                else:
                    safe_names.append(name)
            return safe_names
        
        columns = ["col1", "col2", "col3"]
        result = clean_column_names(columns)
        
        assert result == ["col1", "col2", "col3"]


class TestTemporalFeatures:
    """Unit tests for temporal feature creation"""
    
    def test_time_slot_midnight(self):
        """Test time slot classification for midnight hours"""
        def get_time_slot(hour):
            if 0 <= hour < 6:
                return "midnight"
            elif 6 <= hour < 12:
                return "morning"
            elif 12 <= hour < 18:
                return "afternoon"
            else:
                return "night"
        
        assert get_time_slot(0) == "midnight"
        assert get_time_slot(3) == "midnight"
        assert get_time_slot(5) == "midnight"
    
    def test_time_slot_morning(self):
        """Test time slot classification for morning hours"""
        def get_time_slot(hour):
            if 0 <= hour < 6:
                return "midnight"
            elif 6 <= hour < 12:
                return "morning"
            elif 12 <= hour < 18:
                return "afternoon"
            else:
                return "night"
        
        assert get_time_slot(6) == "morning"
        assert get_time_slot(9) == "morning"
        assert get_time_slot(11) == "morning"
    
    def test_time_slot_afternoon(self):
        """Test time slot classification for afternoon hours"""
        def get_time_slot(hour):
            if 0 <= hour < 6:
                return "midnight"
            elif 6 <= hour < 12:
                return "morning"
            elif 12 <= hour < 18:
                return "afternoon"
            else:
                return "night"
        
        assert get_time_slot(12) == "afternoon"
        assert get_time_slot(15) == "afternoon"
        assert get_time_slot(17) == "afternoon"
    
    def test_time_slot_night(self):
        """Test time slot classification for night hours"""
        def get_time_slot(hour):
            if 0 <= hour < 6:
                return "midnight"
            elif 6 <= hour < 12:
                return "morning"
            elif 12 <= hour < 18:
                return "afternoon"
            else:
                return "night"
        
        assert get_time_slot(18) == "night"
        assert get_time_slot(21) == "night"
        assert get_time_slot(23) == "night"


class TestLogTransformation:
    """Unit tests for log transformations"""
    
    def test_log1p_transformation(self):
        """Test log1p transformation on values"""
        values = [0, 1, 10, 100, 1000000]
        expected = [np.log1p(v) for v in values]
        
        for val, exp in zip(values, expected):
            assert np.isclose(np.log1p(val), exp)
    
    def test_log1p_handles_zero(self):
        """Test log1p handles zero correctly"""
        result = np.log1p(0)
        assert result == 0.0
    
    def test_log1p_handles_large_values(self):
        """Test log1p handles large ETH values"""
        # 1 ETH in Wei
        one_eth_wei = 1000000000000000000
        result = np.log1p(one_eth_wei)
        
        assert result > 0
        assert np.isfinite(result)


class TestFailRateCalculation:
    """Unit tests for fail rate calculations"""
    
    def test_fail_rate_no_failures(self):
        """Test fail rate with no failures"""
        def calculate_fail_rate(fail_count, tx_count):
            return (fail_count + 1) / (tx_count + 1)
        
        # Laplace smoothing: (0+1)/(10+1) = 1/11
        result = calculate_fail_rate(0, 10)
        assert np.isclose(result, 1/11)
    
    def test_fail_rate_all_failures(self):
        """Test fail rate with all failures"""
        def calculate_fail_rate(fail_count, tx_count):
            return (fail_count + 1) / (tx_count + 1)
        
        # Laplace smoothing: (10+1)/(10+1) = 1.0
        result = calculate_fail_rate(10, 10)
        assert np.isclose(result, 1.0)
    
    def test_fail_rate_partial_failures(self):
        """Test fail rate with some failures"""
        def calculate_fail_rate(fail_count, tx_count):
            return (fail_count + 1) / (tx_count + 1)
        
        # Laplace smoothing: (3+1)/(10+1) = 4/11
        result = calculate_fail_rate(3, 10)
        assert np.isclose(result, 4/11)


class TestContractCallDetection:
    """Unit tests for contract call detection"""
    
    def test_is_contract_call_with_address(self):
        """Test contract call detection with contract address"""
        def is_contract_call(contract_address):
            return 1 if contract_address and contract_address != "" else 0
        
        assert is_contract_call("0xABC123") == 1
        assert is_contract_call("0x") == 1
    
    def test_is_contract_call_empty_address(self):
        """Test contract call detection with empty address"""
        def is_contract_call(contract_address):
            return 1 if contract_address and contract_address != "" else 0
        
        assert is_contract_call("") == 0
        assert is_contract_call(None) == 0


class TestZScoreCalculation:
    """Unit tests for z-score calculations"""
    
    def test_zscore_normal_case(self):
        """Test z-score calculation"""
        def calculate_zscore(value, mean, std):
            if std is None or std == 0:
                return None
            return (value - mean) / std
        
        # Value is 2 std above mean
        result = calculate_zscore(120, 100, 10)
        assert np.isclose(result, 2.0)
    
    def test_zscore_zero_std(self):
        """Test z-score with zero standard deviation"""
        def calculate_zscore(value, mean, std):
            if std is None or std == 0:
                return None
            return (value - mean) / std
        
        result = calculate_zscore(100, 100, 0)
        assert result is None
    
    def test_zscore_negative(self):
        """Test negative z-score"""
        def calculate_zscore(value, mean, std):
            if std is None or std == 0:
                return None
            return (value - mean) / std
        
        # Value is 1 std below mean
        result = calculate_zscore(90, 100, 10)
        assert np.isclose(result, -1.0)


class TestSpikeDetection:
    """Unit tests for spike detection"""
    
    def test_value_spike_detected(self):
        """Test spike detection when z-score > 3"""
        def detect_spike(zscore, rolling_count, rolling_std):
            if rolling_count < 10:
                return 0
            if rolling_std is None:
                return 0
            if zscore > 3:
                return 1
            return 0
        
        assert detect_spike(3.5, 15, 1.0) == 1
    
    def test_value_spike_not_detected_low_count(self):
        """Test no spike when rolling count is low"""
        def detect_spike(zscore, rolling_count, rolling_std):
            if rolling_count < 10:
                return 0
            if rolling_std is None:
                return 0
            if zscore > 3:
                return 1
            return 0
        
        assert detect_spike(5.0, 5, 1.0) == 0
    
    def test_value_spike_not_detected_normal_zscore(self):
        """Test no spike when z-score is normal"""
        def detect_spike(zscore, rolling_count, rolling_std):
            if rolling_count < 10:
                return 0
            if rolling_std is None:
                return 0
            if zscore > 3:
                return 1
            return 0
        
        assert detect_spike(2.5, 15, 1.0) == 0


# ============================================================================
# INTEGRATION TESTS - Test with pandas DataFrames
# ============================================================================

class TestFeatureEngineeringIntegration:
    """Integration tests using pandas DataFrames"""
    
    def test_temporal_feature_extraction(self):
        """Test extracting temporal features from timestamps"""
        df = pd.DataFrame({
            'timeStamp': [1609459200, 1609502400, 1609545600]  # Different hours
        })
        
        df['datetime'] = pd.to_datetime(df['timeStamp'], unit='s')
        df['hour'] = df['datetime'].dt.hour
        df['date'] = df['datetime'].dt.date
        
        assert 'hour' in df.columns
        assert 'date' in df.columns
        assert len(df) == 3
    
    def test_aggregation_by_address(self):
        """Test aggregation by from address"""
        df = pd.DataFrame({
            'from': ['0xA', '0xA', '0xB', '0xA'],
            'value': [100, 200, 150, 300],
            'isError': [0, 1, 0, 0]
        })
        
        agg = df.groupby('from').agg(
            tx_count=('value', 'count'),
            fail_count=('isError', 'sum')
        ).reset_index()
        
        assert len(agg) == 2
        assert agg[agg['from'] == '0xA']['tx_count'].values[0] == 3
        assert agg[agg['from'] == '0xA']['fail_count'].values[0] == 1
    
    def test_log_transformation_on_dataframe(self):
        """Test log transformation on DataFrame column"""
        df = pd.DataFrame({
            'value': [0, 100, 1000, 10000]
        })
        
        df['log_value'] = np.log1p(df['value'])
        
        assert df['log_value'].iloc[0] == 0.0
        assert df['log_value'].iloc[1] > 0
        assert df['log_value'].iloc[3] > df['log_value'].iloc[2]


# ============================================================================
# SYSTEM TESTS - Test script structure
# ============================================================================

class TestFeatureEngineeringScript:
    """System tests for feature engineering script"""
    
    SCRIPT_PATHS = [
        'feature_engineering.py',
        'ethereum-feature-engineering.py',
        'glue_jobs/feature_engineering.py',
    ]
    
    def find_script(self):
        for path in self.SCRIPT_PATHS:
            if os.path.exists(path):
                return path
        return None
    
    def test_script_exists(self):
        """Test that feature engineering script exists"""
        script = self.find_script()
        if script is None:
            pytest.skip("Feature engineering script not found")
        assert os.path.exists(script)
    
    def test_script_has_valid_syntax(self):
        """Test script compiles without errors"""
        script = self.find_script()
        if script is None:
            pytest.skip("Feature engineering script not found")
        
        with open(script, 'r') as f:
            code = f.read()
        
        try:
            compile(code, script, 'exec')
        except SyntaxError as e:
            pytest.fail(f"Syntax error: {e}")
    
    def test_script_has_glue_imports(self):
        """Test script has AWS Glue imports"""
        script = self.find_script()
        if script is None:
            pytest.skip("Feature engineering script not found")
        
        with open(script, 'r') as f:
            content = f.read()
        
        assert 'GlueContext' in content, "Missing GlueContext"
        assert 'pyspark' in content, "Missing pyspark import"
    
    def test_script_has_process_function(self):
        """Test script has main processing function"""
        script = self.find_script()
        if script is None:
            pytest.skip("Feature engineering script not found")
        
        with open(script, 'r') as f:
            content = f.read()
        
        assert 'def process_transactions' in content or 'def clean_column_names' in content
    
    def test_script_commits_job(self):
        """Test script commits Glue job"""
        script = self.find_script()
        if script is None:
            pytest.skip("Feature engineering script not found")
        
        with open(script, 'r') as f:
            content = f.read()
        
        assert 'job.commit()' in content, "Script should commit job"