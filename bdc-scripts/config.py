import os 

AWS_KEY_ID = os.environ.get('KEY_ID', '')
AWS_SECRET_KEY = os.environ.get('SECRET_KEY', '')

HOST = os.environ.get('RDS_HOST', '')
DBNAME = os.environ.get('RDS_DBNAME', '')
USER = os.environ.get('RDS_USER', '')
PASSWORD = os.environ.get('RDS_PASSWORD', '')

URL_STAC = os.environ.get('URL_STAC', '')

BUCKET_NAME = os.environ.get('BUCKET_NAME', '')
LAMBDA_FUNCTION_NAME = os.environ.get('LAMBDA_FUNCTION_NAME', '')
QUEUE_NAME = os.environ.get('QUEUE_NAME', '')
KINESIS_NAME = os.environ.get('KINESIS_NAME', '')
DYNAMO_TB_ACTIVITY = os.environ.get('DYNAMO_TB_ACTIVITY', '')
DBNAME_TB_CONTROL = os.environ.get('DBNAME_TB_CONTROL', '')
