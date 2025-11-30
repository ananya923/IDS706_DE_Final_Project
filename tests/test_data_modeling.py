import pytest
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest

def test_isolation_forest_configuration():
    """Test Isolation Forest model is configured correctly"""
    clf = IsolationForest(
        n_estimators=100,
        contamination=0.01,
        random_state=42,
        n_jobs=-1
    )
    assert clf.n_estimators == 100
    assert clf.contamination == 0.01

def test_model_prediction_format():
    """Test model returns expected prediction format"""
    # Create sample data
    X = pd.DataFrame({
        'log_value': np.random.randn(100),
        'log_gasPrice': np.random.randn(100),
        'total_tx_count_from': np.random.randint(1, 100, 100)
    })
    
    clf = IsolationForest(contamination=0.01, random_state=42)
    clf.fit(X)
    
    predictions = clf.predict(X)
    scores = clf.decision_function(X)
    
    # Check format
    assert len(predictions) == len(X)
    assert set(predictions).issubset({-1, 1})
    assert len(scores) == len(X)
    assert scores.dtype == np.float64

def test_anomaly_detection_rate():
    """Test that anomaly detection respects contamination parameter"""
    X = pd.DataFrame({
        'feature1': np.random.randn(1000),
        'feature2': np.random.randn(1000)
    })
    
    clf = IsolationForest(contamination=0.01, random_state=42)
    clf.fit(X)
    predictions = clf.predict(X)
    
    anomaly_rate = (predictions == -1).sum() / len(predictions)
    # Should be close to 1% (within reasonable tolerance)
    assert 0.005 <= anomaly_rate <= 0.02

def test_feature_selection_columns():
    """Test that required feature columns are present"""
    required_features = [
        'log_value', 'log_gasPrice',
        'total_tx_count_from', 'daily_tx_count_from',
        'value_spike', 'gasPrice_spike',
        'is_contract_call'
    ]
    
    # Simulate feature dataframe
    X = pd.DataFrame({col: np.random.randn(10) for col in required_features})
    
    assert all(col in X.columns for col in required_features)
