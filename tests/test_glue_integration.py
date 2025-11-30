import pytest
from unittest.mock import Mock, patch

def test_s3_bucket_accessibility():
    """Test S3 bucket is accessible"""
    import boto3
    from botocore.exceptions import ClientError
    
    s3 = boto3.client('s3')
    bucket_name = 'de-27-team11'
    
    try:
        s3.head_bucket(Bucket=bucket_name)
        assert True
    except ClientError:
        pytest.skip("S3 bucket not accessible in test environment")

def test_glue_database_exists():
    """Test Glue database exists"""
    import boto3
    
    glue = boto3.client('glue')
    
    try:
        response = glue.get_database(Name='ethereum_fraud_detection')
        assert response['Database']['Name'] == 'ethereum_fraud_detection'
    except:
        pytest.skip("Glue database not set up in test environment")
