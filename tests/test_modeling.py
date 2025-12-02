import pytest
from unittest.mock import Mock, patch
import sys

def test_modeling_script_exists():
    """Test that modeling script exists and can be read"""
    try:
        with open('scripts/glue_modeling.py', 'r') as f:
            content = f.read()
            assert 'IsolationForest' in content
            assert 'train_isolation_forest_model' in content
            assert 'contamination=0.01' in content
    except FileNotFoundError:
        pytest.fail("Modeling script not found")

def test_model_configuration():
    """Test that model uses correct configuration"""
    with open('scripts/glue_modeling.py', 'r') as f:
        content = f.read()
        # Check key parameters
        assert 'n_estimators=100' in content
        assert 'contamination=0.01' in content
        assert 'anomaly_score' in content

def test_feature_list_defined():
    """Test that feature columns are properly defined"""
    with open('scripts/glue_modeling.py', 'r') as f:
        content = f.read()
        # Check important features are included
        assert 'log_value' in content
        assert 'log_gasPrice' in content
        assert 'total_tx_count_from' in content
        assert 'value_spike' in content
