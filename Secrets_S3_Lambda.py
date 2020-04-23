import snowflake.connector as sf
import boto3
import base64
from botocore.exceptions import ClientError
import json
from boto3 import client as boto3_client
lambda_client = boto3_client('lambda')

def snf_conn(connection, query):
        cursor = connection.cursor()
        cursor.execute(query)
        cursor.close()

def get_secret(event=None,context=None):

    secret_name = "sm-snowflake-integration-dev"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )


    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
            username = secret['username']
            password = secret['password']
            account = secret['account']
            warehouse = 'WAREHOUSE'
            database = 'CLMS_DEV'
            schema = 'MTRO'
            AWS_REGION = secret['AWS_REGION']
            AWS_ACCESS_KEY_ID = secret['AWS_ACCESS_KEY_ID'] 
            AWS_SECRET_ACCESS_KEY = secret['AWS_SECRET_ACCESS_KEY']
            MANDRILL_API_KEY = secret['MANDRILL_API_KEY']
            LAMBDA_NAME = secret['LAMBDA_NAME']
            conn = sf.connect(user=username, password=password, account=account, schema=schema)
            cursor = conn.cursor()
            sql = '''copy into abc from @pec/PS.csv file_format = (format_name = ff_csv_format) on_error = 'skip_file'; '''
            cursor.execute(sql)
            result = cursor.fetchall()
            tup = result[0]
            #print(tup)
            #print(len(tup))
            email = [{'email':'pravin@xxx','type':'to'},{'email':'xxxx@xxxx','type':'to'},{'email':'xxx@xxx','type':'to'}]
            if len(tup) >= 2:
                file_name = tup[0]
                status = tup[1]
                row_loaded = tup[3]
                error_count = tup[5]
                first_error = tup[6]
                first_error_line = tup[7]
                mandrilMessage = {'to': email,
                      'subject': "csv is loaded",
                      'from_name': 'Pravin Pande',
                      'from_email': 'pravin@xxxx',
                      'html': """\
                           <html>
                           <head></head>
                           <body>
                           <p>Hi!
                           <br>File Name: """ +str(file_name)+ """  
                           <br>Status: """ +str(status)+ """
                           <br>Rows Loaded: """ +str(row_loaded)+ """
                           <br>Error Count: """ +str(error_count)+ """
                           <br>First Error: """ +str(first_error)+ """
                           <br>First Error Line: """ +str(first_error_line)+ """
                           </p>
                           </body>
                           </html>
                           """
                      }
                dicta = {'MANDRILL_API_KEY':MANDRILL_API_KEY,'mandrillMessage':mandrilMessage}
                datatosend = json.dumps(dicta)
                invoke = lambda_client.invoke(FunctionName=LAMBDA_NAME,
                                          Payload=datatosend,
                                          InvocationType='RequestResponse')

            else:
                file_name = result[0][0]
                mandrilMessage = {'to': email,
                      'subject': "csv is NOT loaded",
                      'from_name': 'Pravin Pande',
                      'from_email': 'pravin@xxxx',
                      'html': """\
                           <html>
                           <head></head>
                           <body>
                           <p>Hi!  """ +str(file_name)+ """ file was not loaded<br>
                           </p>
                           </body>
                           </html>
                           """
                      }
                dicta = {'MANDRILL_API_KEY':MANDRILL_API_KEY,'mandrillMessage':mandrilMessage}
                datatosend = json.dumps(dicta)
                invoke = lambda_client.invoke(FunctionName=LAMBDA_NAME,
                                          Payload=datatosend,
                                          InvocationType='RequestResponse')            
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])          
    return tup
            
print(get_secret())
