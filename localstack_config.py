import boto3
import json
import os

# Configuración para LocalStack
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
endpoint_url = 'http://localhost:4566'

# Crear el cliente para Secrets Manager
secrets_manager_client = boto3.client('secretsmanager', endpoint_url=endpoint_url)

# Crear el secreto en Secrets Manager
secret_name = "database"
secret_value = json.dumps({"DATABASE_USER": "admin", "DATABASE_PASSWORD": "admin"})
secrets_manager_client.create_secret(Name=secret_name, SecretString=secret_value)


# crear otro secreto Secret Manager
secret_name = "NGMF_RDS_POSTGRES_CREDENTIALS"
secret_value = json.dumps({"USERNAME": "postgres", "PASSWORD": "postgres"})
secrets_manager_client.create_secret(Name=secret_name, SecretString=secret_value)

# Crear el cliente para SSM (Simple Systems Manager)
ssm_client = boto3.client('ssm', endpoint_url=endpoint_url)

# Crear el parámetro en SSM con el nombre especificado
parameter_name = "gmf/transversal/config-retries"
parameter_value = json.dumps({"number-retries": "5", "time-between-retry": "900"})
ssm_client.put_parameter(Name=parameter_name, Value=parameter_value, Type='String')

parameter_name = "gmf/process-responses/config-retries"
parameter_value = json.dumps({
 "time-between-retry": 120,
 "start-special-files":"RE_ESP_TUTGMF00010039",
 "end-special-files": "-0001"
})
ssm_client.put_parameter(Name=parameter_name, Value=parameter_value, Type='String')

parameter_name = "ngmf/transversal/config-retries"
parameter_value = json.dumps({"number-retries": "5", "time-between-retry": "900"})
ssm_client.put_parameter(Name=parameter_name, Value=parameter_value, Type='String')

parameter_name = "ngmf/process-responses/config-retries"
parameter_value = json.dumps({
    "time-between-retry": 120,
    "start-special-files": "RE_ESP_TUTGMF00010039",
    "end-special-files": "-0001"
})
ssm_client.put_parameter(Name=parameter_name, Value=parameter_value, Type='String')

# Crear el cliente para S3
s3_client = boto3.client('s3', endpoint_url=endpoint_url)

# Crear el bucket en S3
bucket_name = "01-bucketrtaprocesa-d01"
s3_client.create_bucket(Bucket=bucket_name)

# Crear las carpetas en el bucket
# folders = ["Recibidos/", "Procesando/", "Rechazados/", "Procesados/"]
# for folder in folders:
#     s3_client.put_object(Bucket=bucket_name, Key=folder)

# Crear las carpetas en el bucket con un archivo .keep para evitar objetos vacíos
folders = ["Recibidos/", "Procesando/", "Rechazados/", "Procesados/"]
for folder in folders:
    s3_client.put_object(Bucket=bucket_name, Key=f"{folder}.keep", Body="")


# Crear el cliente para SQS
sqs_client = boto3.client('sqs', endpoint_url=endpoint_url)

# Crear las colas en SQS
queues = ["pro-responses-to-process", "pro-responses-to-validate", "pro-responses-to-consolidate","emails-to-send", "pro-responses-to-reception"]
for queue in queues:
    sqs_client.create_queue(QueueName=queue)

# Mensaje final
print("Recursos AWS creados con Éxito")
