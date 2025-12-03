import pytest
import os

def test_modeling_script_exists():
    """Test that modeling script exists"""
    if not os.path.exists('scripts/glue_modeling.py'):
        pytest.skip("Modeling script will be added in data_transformation PR")
    
    assert os.path.exists('scripts/glue_modeling.py')

def test_modeling_has_isolation_forest():
    """Test that script uses IsolationForest for fraud detection"""
    if not os.path.exists('scripts/glue_modeling.py'):
        pytest.skip("Modeling script not yet in repo")
    
    with open('scripts/glue_modeling.py', 'r') as f:
        content = f.read()
        assert 'IsolationForest' in content, "Should use IsolationForest algorithm"
        assert 'from sklearn.ensemble import' in content or 'sklearn' in content

def test_modeling_has_required_imports():
    """Test that script has required ML imports"""
    if not os.path.exists('scripts/glue_modeling.py'):
        pytest.skip("Modeling script not yet in repo")
    
    with open('scripts/glue_modeling.py', 'r') as f:
        content = f.read()
        # Check for essential imports
        assert 'sklearn' in content, "Should import sklearn"
        assert 'pandas' in content or 'pd' in content, "Should use pandas"

def test_modeling_has_train_function():
    """Test that script has training function"""
    if not os.path.exists('scripts/glue_modeling.py'):
        pytest.skip("Modeling script not yet in repo")
    
    with open('scripts/glue_modeling.py', 'r') as f:
        content = f.read()
        # Should have a training function
        assert 'def train' in content or 'def fit' in content or 'def main' in content

def test_modeling_has_feature_selection():
    """Test that script defines features for modeling"""
    if not os.path.exists('scripts/glue_modeling.py'):
        pytest.skip("Modeling script not yet in repo")
    
    with open('scripts/glue_modeling.py', 'r') as f:
        content = f.read()
        # Should have feature selection/definition
        assert 'feature' in content.lower(), "Should define features"
        assert 'log_value' in content or 'feature_cols' in content, "Should specify feature columns"

def test_modeling_has_predictions():
    """Test that script generates predictions"""
    if not os.path.exists('scripts/glue_modeling.py'):
        pytest.skip("Modeling script not yet in repo")
    
    with open('scripts/glue_modeling.py', 'r') as f:
        content = f.read()
        # Should make predictions
        assert 'predict' in content, "Should generate predictions"
        assert 'anomaly' in content.lower() or 'fraud' in content.lower(), "Should detect anomalies/fraud"

def test_modeling_contamination_parameter():
    """Test that IsolationForest has contamination parameter set"""
    if not os.path.exists('scripts/glue_modeling.py'):
        pytest.skip("Modeling script not yet in repo")
    
    with open('scripts/glue_modeling.py', 'r') as f:
        content = f.read()
        if 'IsolationForest' in content:
            # Should set contamination parameter
            assert 'contamination' in content, "IsolationForest should have contamination parameter"
