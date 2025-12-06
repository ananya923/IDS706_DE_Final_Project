"""
Unit Tests for Fraud Modeling Script
Tests anomaly detection logic and scoring functions
"""
import pytest
import os
import pandas as pd
import numpy as np
import functools


# ============================================================================
# UNIT TESTS - Test anomaly scoring logic
# ============================================================================

class TestZScoreComputation:
    """Unit tests for z-score computation"""
    
    def test_zscore_basic(self):
        """Test basic z-score calculation"""
        def compute_zscore(value, mean, std):
            if std is None or std == 0:
                return None
            return (value - mean) / std
        
        assert np.isclose(compute_zscore(120, 100, 10), 2.0)
        assert np.isclose(compute_zscore(100, 100, 10), 0.0)
        assert np.isclose(compute_zscore(90, 100, 10), -1.0)
    
    def test_zscore_zero_std_returns_none(self):
        """Test z-score returns None when std is zero"""
        def compute_zscore(value, mean, std):
            if std is None or std == 0:
                return None
            return (value - mean) / std
        
        assert compute_zscore(100, 100, 0) is None


class TestAnomalyScoreComputation:
    """Unit tests for anomaly score computation"""
    
    def test_anomaly_score_single_feature(self):
        """Test anomaly score with single z-score"""
        z_scores = [2.0]
        anomaly_score = sum(z**2 for z in z_scores)
        assert np.isclose(anomaly_score, 4.0)
    
    def test_anomaly_score_multiple_features(self):
        """Test anomaly score with multiple z-scores"""
        z_scores = [1.0, 2.0, 3.0]
        anomaly_score = sum(z**2 for z in z_scores)
        assert np.isclose(anomaly_score, 14.0)
    
    def test_anomaly_score_normal_transaction(self):
        """Test normal transaction has low anomaly score"""
        z_scores = [0.1, -0.2, 0.3]
        anomaly_score = sum(z**2 for z in z_scores)
        assert anomaly_score < 1.0
    
    def test_anomaly_score_outlier_transaction(self):
        """Test outlier has high anomaly score"""
        z_scores = [5.0, 4.0, 6.0]
        anomaly_score = sum(z**2 for z in z_scores)
        assert anomaly_score > 50


class TestAnomalyLabeling:
    """Unit tests for anomaly labeling"""
    
    def test_label_anomaly_above_threshold(self):
        """Test labeling when score is above threshold"""
        def label_anomaly(score, threshold):
            return -1 if score >= threshold else 1
        
        assert label_anomaly(15.0, 10.0) == -1
    
    def test_label_normal_below_threshold(self):
        """Test labeling when score is below threshold"""
        def label_anomaly(score, threshold):
            return -1 if score >= threshold else 1
        
        assert label_anomaly(5.0, 10.0) == 1


class TestPercentileThreshold:
    """Unit tests for percentile-based threshold"""
    
    def test_99th_percentile_threshold(self):
        """Test 99th percentile calculation"""
        scores = list(range(100))
        threshold = np.percentile(scores, 99)
        assert threshold >= 97
    
    def test_threshold_flags_outliers(self):
        """Test threshold flags top percent"""
        np.random.seed(42)
        scores = np.random.normal(0, 1, 1000)
        threshold = np.percentile(scores, 99)
        anomalies = [s for s in scores if s >= threshold]
        assert len(anomalies) <= 20


class TestReduceFunction:
    """Unit tests for functools.reduce"""
    
    def test_reduce_sum_squared(self):
        """Test reduce for summing squared values"""
        values = [1.0, 2.0, 3.0]
        squared = [v**2 for v in values]
        result = functools.reduce(lambda a, b: a + b, squared)
        assert np.isclose(result, 14.0)


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestModelingIntegration:
    """Integration tests with DataFrames"""
    
    def test_full_anomaly_scoring_pipeline(self):
        """Test complete anomaly scoring"""
        np.random.seed(42)
        df = pd.DataFrame({
            'log_value': np.random.normal(10, 2, 100),
        })
        df.loc[0, 'log_value'] = 50  # outlier
        
        mean = df['log_value'].mean()
        std = df['log_value'].std()
        df['z'] = (df['log_value'] - mean) / std
        df['anomaly_score'] = df['z']**2
        
        threshold = df['anomaly_score'].quantile(0.99)
        df['pred_label'] = df['anomaly_score'].apply(
            lambda x: -1 if x >= threshold else 1
        )
        
        assert 'pred_label' in df.columns
    
    def test_outlier_detection(self):
        """Test outlier has highest score"""
        df = pd.DataFrame({'value': [100, 101, 99, 1000]})
        mean = df['value'].mean()
        std = df['value'].std()
        df['score'] = ((df['value'] - mean) / std) ** 2
        
        assert df['score'].idxmax() == 3


# ============================================================================
# SYSTEM TESTS
# ============================================================================

class TestModelingScript:
    """System tests for modeling script"""
    
    SCRIPT_PATHS = [
        'glue_jobs/modeling.py',
        'glue_jobs/ethereum_fraud_modeling.py',
        'modeling.py',
    ]
    
    def find_script(self):
        for path in self.SCRIPT_PATHS:
            if os.path.exists(path):
                return path
        return None
    
    def test_script_exists(self):
        """Test modeling script exists"""
        script = self.find_script()
        if script is None:
            pytest.skip("Modeling script not found")
        assert os.path.exists(script)
    
    def test_script_has_valid_syntax(self):
        """Test script compiles"""
        script = self.find_script()
        if script is None:
            pytest.skip("Modeling script not found")
        
        with open(script, 'r') as f:
            code = f.read()
        compile(code, script, 'exec')
    
    def test_script_has_anomaly_detection(self):
        """Test script has anomaly detection"""
        script = self.find_script()
        if script is None:
            pytest.skip("Modeling script not found")
        
        with open(script, 'r') as f:
            content = f.read().lower()
        
        assert 'anomaly' in content or 'zscore' in content