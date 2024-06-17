import boto3
import json
def get_credentials(secretname):
    credential = {}
    
    secret_name = secretname
    region_name = "ap-southeast-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    get_secret_value_response = client.get_secret_value(
      SecretId=secret_name
    )
    secret = json.loads(get_secret_value_response['SecretString'])
    credential['username'] = secret['username']
    credential['password'] = secret['password']
    credential['port'] = secret['port'] 
    credential['host'] = secret['host'] 
    credential['catalogdb'] = secret['catalogdb'] # tên kết nối
    credential['role'] = secret['role'] # tên luật
    
    if "url" in secret:
        credential['url'] = secret['url']
    else:
        credential['url'] = ""

    if "s3bucket" in secret:
        credential['s3bucket'] = secret['s3bucket']
    
    if "catalogdb" in secret:
        credential['catalogdb'] = secret['catalogdb']
        
    if "dbInstanceIdentifier" in secret:
        credential['dbInstanceIdentifier'] = secret['dbInstanceIdentifier']   
       
    if "dbname" in secret:
        credential['dbname'] = secret['dbname']
        
    if "schemaName" in secret:
        credential['schemaName'] = secret['schemaName']

    if "role" in secret:
        credential['role'] = secret['role']
        
       
    return credential
    