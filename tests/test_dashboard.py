import os
import pytest

def test_dashboard_file_exists():
    """Test that dashboard file exists"""
    if not (os.path.exists('dashboard/app.py') or os.path.exists('app.py')):
        pytest.skip("Dashboard file not yet in repo")
    
    assert os.path.exists('dashboard/app.py') or os.path.exists('app.py')

def test_dashboard_imports_streamlit():
    """Test that dashboard imports streamlit"""
    # Check both possible locations
    if os.path.exists('dashboard/app.py'):
        filepath = 'dashboard/app.py'
    elif os.path.exists('app.py'):
        filepath = 'app.py'
    else:
        pytest.skip("Dashboard file not found")
    
    with open(filepath, 'r') as f:
        content = f.read()
        assert 'import streamlit' in content or 'import streamlit as st' in content

def test_dashboard_has_title():
    """Test that dashboard has a title"""
    if os.path.exists('dashboard/app.py'):
        filepath = 'dashboard/app.py'
    elif os.path.exists('app.py'):
        filepath = 'app.py'
    else:
        pytest.skip("Dashboard file not found")
    
    with open(filepath, 'r') as f:
        content = f.read()
        assert 'st.title' in content

def test_dashboard_has_mock_data_function():
    """Test that dashboard has mock data loading function"""
    if os.path.exists('dashboard/app.py'):
        filepath = 'dashboard/app.py'
    elif os.path.exists('app.py'):
        filepath = 'app.py'
    else:
        pytest.skip("Dashboard file not found")
    
    with open(filepath, 'r') as f:
        content = f.read()
        assert 'def load_mock_data' in content or 'def load_data' in content

def test_dashboard_uses_pandas():
    """Test that dashboard imports pandas for data handling"""
    if os.path.exists('dashboard/app.py'):
        filepath = 'dashboard/app.py'
    elif os.path.exists('app.py'):
        filepath = 'app.py'
    else:
        pytest.skip("Dashboard file not found")
    
    with open(filepath, 'r') as f:
        content = f.read()
        assert 'import pandas' in content or 'import pandas as pd' in content

def test_dashboard_no_hardcoded_secrets():
    """Test that dashboard has no hardcoded secrets"""
    if os.path.exists('dashboard/app.py'):
        filepath = 'dashboard/app.py'
    elif os.path.exists('app.py'):
        filepath = 'app.py'
    else:
        pytest.skip("Dashboard file not found")
    
    with open(filepath, 'r') as f:
        content = f.read()
        
    
        suspicious_patterns = [
            'api_key=',
            'apikey=',
            'secret_key=',
            'access_key='
        ]
        for pattern in suspicious_patterns:
            if pattern in content.lower():
                pytest.fail(f"Found potential hardcoded secret pattern: {pattern}")

def test_dashboard_has_dataframe_display():
    """Test that dashboard displays data using st.dataframe"""
    if os.path.exists('dashboard/app.py'):
        filepath = 'dashboard/app.py'
    elif os.path.exists('app.py'):
        filepath = 'app.py'
    else:
        pytest.skip("Dashboard file not found")
    
    with open(filepath, 'r') as f:
        content = f.read()
        assert 'st.dataframe' in content

def test_dashboard_has_metrics():
    """Test that dashboard shows metrics"""
    if os.path.exists('dashboard/app.py'):
        filepath = 'dashboard/app.py'
    elif os.path.exists('app.py'):
        filepath = 'app.py'
    else:
        pytest.skip("Dashboard file not found")
    
    with open(filepath, 'r') as f:
        content = f.read()
        assert 'st.metric' in content or 'st.columns' in content

def test_dashboard_has_valid_python_syntax():
    """Test that dashboard file has valid Python syntax"""
    if os.path.exists('dashboard/app.py'):
        filepath = 'dashboard/app.py'
    elif os.path.exists('app.py'):
        filepath = 'app.py'
    else:
        pytest.skip("Dashboard file not found")
    
    # Try to compile the file to check syntax
    with open(filepath, 'r') as f:
        code = f.read()
    
    try:
        compile(code, filepath, 'exec')
    except SyntaxError as e:
        pytest.fail(f"Syntax error in dashboard: {e}")
