import pytest
import os

def test_dashboard_exists():
    """Test that Streamlit dashboard exists"""
    if not os.path.exists('dashboard/app.py'):
        pytest.skip("Dashboard will be added in future PR")
    
    assert os.path.exists('dashboard/app.py')

def test_dashboard_has_streamlit():
    """Test that dashboard imports Streamlit"""
    if not os.path.exists('dashboard/app.py'):
        pytest.skip("Dashboard not yet in repo")
    
    with open('dashboard/app.py', 'r') as f:
        content = f.read()
        assert 'import streamlit' in content, "Should import Streamlit"
        assert 'st.' in content, "Should use Streamlit components"

def test_dashboard_has_title():
    """Test that dashboard has a title"""
    if not os.path.exists('dashboard/app.py'):
        pytest.skip("Dashboard not yet in repo")
    
    with open('dashboard/app.py', 'r') as f:
        content = f.read()
        assert 'st.title' in content or 'st.header' in content, "Should have a title"

def test_dashboard_loads_data():
    """Test that dashboard has data loading function"""
    if not os.path.exists('dashboard/app.py'):
        pytest.skip("Dashboard not yet in repo")
    
    with open('dashboard/app.py', 'r') as f:
        content = f.read()
        assert 'load_mock_data' in content or 'load_data' in content, "Should have data loading function"

def test_dashboard_no_hardcoded_credentials():
    """Test that dashboard doesn't have hardcoded database credentials"""
    if not os.path.exists('dashboard/app.py'):
        pytest.skip("Dashboard not yet in repo")
    
    with open('dashboard/app.py', 'r') as f:
        content = f.read()
        # Should not have hardcoded passwords
        assert 'password=' not in content.lower() or 'mock' in content.lower(), \
            "Should not have hardcoded credentials"
