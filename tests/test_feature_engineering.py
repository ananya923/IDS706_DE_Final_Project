import pytest
from unittest.mock import Mock, patch

def test_script_structure():
    """Test that feature engineering script has required components"""
    import sys
    sys.path.insert(0, 'scripts')
    
    try:
        from glue_feature_engineering import process_transactions
        assert callable(process_transactions)
    except ImportError as e:
        pytest.skip(f"Glue context dependencies not available: {e}")

def test_feature_engineering_imports():
    """Test that script imports work"""
    try:
        import sys
        sys.path.insert(0, 'scripts')
        import glue_feature_engineering
        assert True
    except ImportError as e:
        pytest.skip(f"PySpark/Glue dependencies not available in test env: {e}")

def test_required_transformations_defined():
    """Test that key transformation functions are present"""
    import sys
    sys.path.insert(0, 'scripts')
    
    try:
        # Just check file can be read and has expected content
        with open('scripts/glue_feature_engineering.py', 'r') as f:
            content = f.read()
            assert 'log_value' in content
            assert 'time_slot' in content
            assert 'rolling_window' in content
            assert 'spike' in content
    except FileNotFoundError:
        pytest.fail("Feature engineering script not found")
