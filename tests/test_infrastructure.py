import pytest
import yaml

def test_cloudformation_template_valid():
    """Test CloudFormation template is valid YAML"""
    with open('temp_ingestion_infrastructure.yaml', 'r') as f:
        template = yaml.safe_load(f)
    
    assert 'AWSTemplateFormatVersion' in template
    assert 'Resources' in template
    assert 'GlueEtherscanRole' in template['Resources']
    assert 'EtherscanIngestionJob' in template['Resources']

def test_glue_job_configuration():
    """Test Glue job has correct configuration"""
    with open('temp_ingestion_infrastructure.yaml', 'r') as f:
        template = yaml.safe_load(f)
    
    job = template['Resources']['EtherscanIngestionJob']
    props = job['Properties']
    
    assert props['Command']['Name'] == 'pythonshell'
    assert props['Command']['PythonVersion'] == '3.9'
    assert 'etherscan_to_s3_glue.py' in props['Command']['ScriptLocation']
    assert props['MaxRetries'] == 1

def test_required_parameters_exist():
    """Test template has required parameters"""
    with open('temp_ingestion_infrastructure.yaml', 'r') as f:
        template = yaml.safe_load(f)
    
    params = template['Parameters']
    assert 'EtherscanApiKey' in params
    assert 'EthereumAddress' in params
    assert params['EtherscanApiKey']['NoEcho'] == True  
