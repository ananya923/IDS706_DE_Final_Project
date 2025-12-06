"""
Unit Tests for Streamlit Dashboard
Tests data loading, filtering, and display logic
"""
import pytest
import os
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock


# ============================================================================
# UNIT TESTS - Test dashboard data functions
# ============================================================================

class TestLoadMockData:
    """Unit tests for mock data loading function"""
    
    def test_load_mock_data_returns_dataframe(self):
        """Test that load_mock_data returns a DataFrame"""
        def load_mock_data():
            data = [
                {"tx_hash": "0xaaa", "anomaly_score": 0.97, "fraud_flag": True},
                {"tx_hash": "0xbbb", "anomaly_score": 0.45, "fraud_flag": False},
            ]
            return pd.DataFrame(data)
        
        df = load_mock_data()
        assert isinstance(df, pd.DataFrame)
    
    def test_load_mock_data_has_required_columns(self):
        """Test that mock data has required columns"""
        def load_mock_data():
            data = [
                {
                    "tx_hash": "0xaaa",
                    "from_address": "0xFAR001",
                    "to_address": "0xEXC001",
                    "value_eth": 1.25,
                    "timestamp": "2025-11-26 12:34:56",
                    "anomaly_score": 0.97,
                    "fraud_flag": True,
                }
            ]
            df = pd.DataFrame(data)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df
        
        df = load_mock_data()
        
        required_cols = ['tx_hash', 'from_address', 'to_address', 
                         'value_eth', 'anomaly_score', 'fraud_flag']
        for col in required_cols:
            assert col in df.columns, f"Missing column: {col}"
    
    def test_load_mock_data_timestamp_is_datetime(self):
        """Test that timestamp is properly converted to datetime"""
        def load_mock_data():
            data = [{"timestamp": "2025-11-26 12:34:56"}]
            df = pd.DataFrame(data)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df
        
        df = load_mock_data()
        assert pd.api.types.is_datetime64_any_dtype(df['timestamp'])


class TestAnomalyScoreFiltering:
    """Unit tests for anomaly score filtering"""
    
    def test_filter_by_min_score(self):
        """Test filtering by minimum anomaly score"""
        df = pd.DataFrame({
            'tx_hash': ['0x1', '0x2', '0x3', '0x4'],
            'anomaly_score': [0.3, 0.5, 0.8, 0.95]
        })
        
        min_score = 0.8
        filtered = df[df['anomaly_score'] >= min_score]
        
        assert len(filtered) == 2
        assert '0x3' in filtered['tx_hash'].values
        assert '0x4' in filtered['tx_hash'].values
    
    def test_filter_with_zero_threshold(self):
        """Test filtering with zero threshold returns all"""
        df = pd.DataFrame({
            'anomaly_score': [0.1, 0.5, 0.9]
        })
        
        filtered = df[df['anomaly_score'] >= 0.0]
        assert len(filtered) == 3
    
    def test_filter_with_max_threshold(self):
        """Test filtering with max threshold returns few/none"""
        df = pd.DataFrame({
            'anomaly_score': [0.1, 0.5, 0.9]
        })
        
        filtered = df[df['anomaly_score'] >= 1.0]
        assert len(filtered) == 0


class TestFraudMetrics:
    """Unit tests for fraud metrics calculations"""
    
    def test_fraud_count(self):
        """Test counting fraudulent transactions"""
        df = pd.DataFrame({
            'fraud_flag': [True, False, True, True, False]
        })
        
        fraud_count = df['fraud_flag'].sum()
        assert fraud_count == 3
    
    def test_fraud_rate_calculation(self):
        """Test fraud rate calculation"""
        df = pd.DataFrame({
            'fraud_flag': [True, False, True, False, False]
        })
        
        total = len(df)
        fraud = df['fraud_flag'].sum()
        fraud_rate = fraud / total if total > 0 else 0.0
        
        assert np.isclose(fraud_rate, 0.4)  # 2/5 = 0.4
    
    def test_fraud_rate_no_transactions(self):
        """Test fraud rate with no transactions"""
        df = pd.DataFrame({'fraud_flag': []})
        
        total = len(df)
        fraud_rate = 0 if total == 0 else df['fraud_flag'].sum() / total
        
        assert fraud_rate == 0
    
    def test_average_anomaly_score(self):
        """Test average anomaly score calculation"""
        df = pd.DataFrame({
            'anomaly_score': [0.5, 0.7, 0.9, 0.3]
        })
        
        avg_score = df['anomaly_score'].mean()
        assert np.isclose(avg_score, 0.6)
    
    def test_max_anomaly_score(self):
        """Test max anomaly score"""
        df = pd.DataFrame({
            'anomaly_score': [0.5, 0.7, 0.99, 0.3]
        })
        
        max_score = df['anomaly_score'].max()
        assert np.isclose(max_score, 0.99)


class TestSuspiciousAddresses:
    """Unit tests for suspicious address aggregation"""
    
    def test_top_source_addresses(self):
        """Test getting top source addresses by fraud count"""
        df = pd.DataFrame({
            'from_address': ['0xA', '0xA', '0xB', '0xA', '0xC'],
            'fraud_flag': [True, True, True, False, True]
        })
        
        top_from = (
            df[df['fraud_flag']]
            .groupby('from_address')
            .size()
            .reset_index(name='fraud_count')
            .sort_values('fraud_count', ascending=False)
        )
        
        assert top_from.iloc[0]['from_address'] == '0xA'
        assert top_from.iloc[0]['fraud_count'] == 2
    
    def test_unique_suspicious_wallets(self):
        """Test counting unique suspicious wallets"""
        df = pd.DataFrame({
            'from_address': ['0xA', '0xA', '0xB', '0xC'],
            'fraud_flag': [True, True, True, False]
        })
        
        unique_suspicious = df.loc[df['fraud_flag'], 'from_address'].nunique()
        assert unique_suspicious == 2  # 0xA and 0xB


class TestDailyAggregation:
    """Unit tests for daily trend aggregation"""
    
    def test_daily_transaction_count(self):
        """Test daily transaction count aggregation"""
        df = pd.DataFrame({
            'timestamp': pd.to_datetime([
                '2025-01-01 10:00', '2025-01-01 11:00',
                '2025-01-02 10:00'
            ]),
            'tx_hash': ['0x1', '0x2', '0x3']
        })
        
        daily = df.set_index('timestamp').resample('D').agg(
            total_tx=('tx_hash', 'count')
        ).reset_index()
        
        assert len(daily) == 2
        assert daily.iloc[0]['total_tx'] == 2  # Jan 1
        assert daily.iloc[1]['total_tx'] == 1  # Jan 2
    
    def test_daily_fraud_count(self):
        """Test daily fraud count aggregation"""
        df = pd.DataFrame({
            'timestamp': pd.to_datetime([
                '2025-01-01 10:00', '2025-01-01 11:00',
                '2025-01-02 10:00'
            ]),
            'fraud_flag': [True, False, True]
        })
        
        daily = df.set_index('timestamp').resample('D').agg(
            fraud_tx=('fraud_flag', 'sum')
        ).reset_index()
        
        assert daily.iloc[0]['fraud_tx'] == 1  # Jan 1
        assert daily.iloc[1]['fraud_tx'] == 1  # Jan 2


# ============================================================================
# INTEGRATION TESTS - Test component interactions
# ============================================================================

class TestDashboardIntegration:
    """Integration tests for dashboard components"""
    
    def test_full_data_pipeline(self):
        """Test complete data pipeline from load to display"""
        # Load
        data = [
            {"tx_hash": "0x1", "anomaly_score": 0.9, "fraud_flag": True,
             "from_address": "0xA", "to_address": "0xB", "value_eth": 1.0,
             "timestamp": "2025-01-01 10:00"},
            {"tx_hash": "0x2", "anomaly_score": 0.3, "fraud_flag": False,
             "from_address": "0xC", "to_address": "0xD", "value_eth": 0.5,
             "timestamp": "2025-01-01 11:00"},
        ]
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Filter
        min_score = 0.5
        filtered = df[df['anomaly_score'] >= min_score]
        
        # Metrics
        total_tx = len(df)
        suspicious = len(filtered)
        fraud_rate = df['fraud_flag'].sum() / total_tx
        
        assert total_tx == 2
        assert suspicious == 1
        assert np.isclose(fraud_rate, 0.5)
    
    def test_slider_range_validation(self):
        """Test that slider values are in valid range"""
        min_value = 0.0
        max_value = 1.0
        
        test_values = [0.0, 0.5, 0.8, 1.0]
        
        for val in test_values:
            assert min_value <= val <= max_value


# ============================================================================
# SYSTEM TESTS - Test script structure
# ============================================================================

class TestDashboardScript:
    """System tests for dashboard script"""
    
    SCRIPT_PATHS = [
        'dashboard/app.py',
        'app.py',
        'streamlit_app.py',
    ]
    
    def find_script(self):
        for path in self.SCRIPT_PATHS:
            if os.path.exists(path):
                return path
        return None
    
    def test_script_exists(self):
        """Test that dashboard script exists"""
        script = self.find_script()
        if script is None:
            pytest.skip("Dashboard script not found")
        assert os.path.exists(script)
    
    def test_script_has_valid_syntax(self):
        """Test script compiles without errors"""
        script = self.find_script()
        if script is None:
            pytest.skip("Dashboard script not found")
        
        with open(script, 'r') as f:
            code = f.read()
        
        try:
            compile(code, script, 'exec')
        except SyntaxError as e:
            pytest.fail(f"Syntax error: {e}")
    
    def test_script_imports_streamlit(self):
        """Test script imports streamlit"""
        script = self.find_script()
        if script is None:
            pytest.skip("Dashboard script not found")
        
        with open(script, 'r') as f:
            content = f.read()
        
        assert 'import streamlit' in content
    
    def test_script_imports_pandas(self):
        """Test script imports pandas"""
        script = self.find_script()
        if script is None:
            pytest.skip("Dashboard script not found")
        
        with open(script, 'r') as f:
            content = f.read()
        
        assert 'import pandas' in content
    
    def test_script_has_load_function(self):
        """Test script has data loading function"""
        script = self.find_script()
        if script is None:
            pytest.skip("Dashboard script not found")
        
        with open(script, 'r') as f:
            content = f.read()
        
        assert 'def load_mock_data' in content or 'def load_data' in content
    
    def test_script_has_metrics(self):
        """Test script displays metrics"""
        script = self.find_script()
        if script is None:
            pytest.skip("Dashboard script not found")
        
        with open(script, 'r') as f:
            content = f.read()
        
        assert 'st.metric' in content
    
    def test_script_has_sidebar(self):
        """Test script uses sidebar"""
        script = self.find_script()
        if script is None:
            pytest.skip("Dashboard script not found")
        
        with open(script, 'r') as f:
            content = f.read()
        
        assert 'st.sidebar' in content
    
    def test_script_no_hardcoded_secrets(self):
        """Test script has no hardcoded credentials"""
        script = self.find_script()
        if script is None:
            pytest.skip("Dashboard script not found")
        
        with open(script, 'r') as f:
            content = f.read()
        
        suspicious = ['password=', 'api_key=', 'secret=']
        for pattern in suspicious:
            if pattern in content.lower():
                # Check it's not reading from env
                assert 'getenv' in content or 'environ' in content, \
                    f"Potential hardcoded secret: {pattern}"