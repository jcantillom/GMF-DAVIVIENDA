"""
Este módulo maneja el flujo principal de procesamiento de archivos y manejo de datos.
"""

import os
from typing import Dict, Type, Any
from src.core.actions import Actions
from src.core.error_handling import ErrorHandling
from src.services.logger_service import LoggerService
from src.services.database_service import DatabaseService
from src.services.s3_service import S3Service
from src.services.secrets_service import SecretsService
from src.services.sqs_service import SQSService
from src.services.parameter_store_service import ParameterStoreService
from src.utils.environment import Environment


def initialize_services() -> Dict[str, Any]:
    """
    Inicializa los servicios necesarios y devuelve un diccionario con las instancias.

    Returns:
        Dict[str, Any]:
            Diccionario con las instancias de los servicios.
    """
    # Instancia el LoggerService para manejar logs
    logger_service: LoggerService = LoggerService(
        debug_mode=os.getenv("DEBUG_MODE", "false").lower() == "true"
    )

    # Define las variables de entorno esperadas y sus tipos de datos
    expected_vars: Dict[str, Type] = {
        "SERVICE_NAME": str,
        "REGION_ZONE": str,
        "SECRET_NAME": str,
        "KEYS_SECRETS": list,
        "PARAMETER_NAMES": list,
        "PARAMETER_NAME_PROCESS_RESPONSE": str,
        "PARAMETER_NAME_TRANSVERSAL": str,
        "DB_HOST": str,
        "DB_PORT": int,
        "DB_NAME": str,
        "BUCKET": str,
        "FOLDER_PROCESSING": str,
        "FOLDER_REJECTED": str,
        "SQS_URL_PRO_RESPONSE_TO_PROCESS": str,
        "SQS_URL_PRO_RESPONSE_TO_VALIDATE": str,
        "SQS_URL_PRO_RESPONSE_TO_CONSOLIDATE": str,
        "SQS_URL_EMAILS": str,
        "ESTADO_ENVIADO": str,
        "ESTADO_PREVALIDADO": str,
        "ESTADO_PROCESAMIENTO_FALLIDO": str,
        "ESTADO_PROCESA_PENDIENTE_REINTENTO": str,
        "ESTADO_PROCESAMIENTO_RECHAZADO": str,
        "CONSTANTE_TU_DEBITO_REVERSO": list,
        "CONSTANTES_TU_REINTEGROS": list,
        "CONSTANTES_TU_ESPECIALES": list,
        "DEBUG_MODE": bool,
        "IS_LOCAL": bool,
        "LOCALSTACK_ENDPOINT": str
    }
    # Instancia el Environment para manejar las variables de entorno
    env: Environment = Environment(logger_service=logger_service, expected_vars=expected_vars)

    # Instancia el SecretsService para manejar los secrets del AWS Secrets Manager
    secrets_service: SecretsService = SecretsService(
        env=env,
        logger_service=logger_service,
        secret_name=env.SECRET_NAME,
        keys_secrets=env.KEYS_SECRETS,
    )

    # Instancia el ParameterStoreService para manejar los parámetros del
    # AWS System Manager Parameter Store
    parameter_store_service: ParameterStoreService = ParameterStoreService(
        env=env,
        logger_service=logger_service,
        parameter_names=env.PARAMETER_NAMES,
    )

    # Instancia el DatabaseService para interactuar con la base de datos
    postgres_service: DatabaseService = DatabaseService(
        env=env,
        secrets_service=secrets_service,
        logger_service=logger_service,
    )

    # Instancia el S3Service para interactuar con los buckets del S3
    s3_service: S3Service = S3Service(env=env, logger_service=logger_service)

    # Instancia el SQSService para interactuar con las colas SQS
    sqs_service: SQSService = SQSService(env=env, logger_service=logger_service)

    return {
        "logger_service": logger_service,
        "env": env,
        "secrets_service": secrets_service,
        "parameter_store_service": parameter_store_service,
        "postgres_service": postgres_service,
        "s3_service": s3_service,
        "sqs_service": sqs_service,
    }


def lambda_handler(event: dict, context: object) -> None:
    """
    Función principal que maneja el evento de AWS Lambda.

    Args:
        event (dict):
            Evento recibido por la función Lambda.
        context (object):
            Contexto de la ejecución de la función Lambda.
    """
    # Inicializa los servicios
    services: Dict[str, Any] = initialize_services()

    # Obtiene la instancia de LoggerService
    logger_service: LoggerService = services["logger_service"]
    # Registra log informativo de inicio de la ejecución
    logger_service.log_info("Inicia ejecucion")

    try:
        # Instancia el ErrorHandling para el manejo de los errores
        error_handling: ErrorHandling = ErrorHandling(
            services=services,
            event_data=event,
        )
        # Instancia el Actions para realizar las validaciones del zip a procesar
        actions: Actions = Actions(
            services=services,
            error_handling=error_handling,
        )
        logger_service.log_info("Procesa cada mensaje en el evento de SQS")

        for record in event["Records"]:
            actions.start_process(record)

    except Exception as e:
        # Registra el log del error no controlado
        logger_service.log_error(f'Error no controlado {{1}}" "1={e}')
        # Registra log informativo de fin de la ejecución
        logger_service.log_info("Finaliza ejecucion con errores")
        raise e


if __name__ == "__main__":
    # Crear un evento de prueba
    event_data = {
        "Records": [
            {
                "messageId": "32a6dcc3-992e-4622-8437-1ff1c11f883c",
                "receiptHandle": "OGI0YjZhM2MtOGYyZS00OTljLWFlMTUtMGVlMDM1MzJjYjY3IGFybjphd3M6c3FzOnVzLWVhc3QtMTowMDAwMDAwMDAwMDA6c3FzLXJlY2VpdmVzIDMyYTZkY2MzLTk5MmUtNDYyMi04NDM3LTFmZjFjMTFmODgzYyAxNzIxNTE0ODg2LjU0NjA3NDY=",
                "body": '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2020-09-25T15:43:27.121Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:EXAMPLE"},"requestParameters":{"sourceIPAddress":"205.255.255.255"},"responseElements":{"x-amz-request-id":"EXAMPLE123456789","x-amz-id-2":"EXAMPLE123/5678ABCDEFGHIJK12345EXAMPLE="},"s3":{"s3SchemaVersion":"1.0","configurationId":"testConfigRule","bucket":{"name":"01-bucketrtaprocesa-d01","ownerIdentity":{"principalId":"EXAMPLE"},"arn":"arn:aws:s3:::example-bucket"},"object":{"key":"Recibidos/RE_PRO_TUTGMF0001003920241004-0001.zip","size":1024,"eTag":"0123456789abcdef0123456789abcdef","sequencer":"0A1B2C3D4E5F678901"}}}]}',
                "attributes": {
                    "ApproximateReceiveCount": "2",
                    "SentTimestamp": "1721084544748",
                    "SenderId": "000000000000",
                    "ApproximateFirstReceiveTimestamp": "1721084544820",
                },
                "messageAttributes": {},
                "md5OfBody": "2fd1355d7becb7a1460d1b5fc54c0095",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:my-queue",
                "awsRegion": "us-east-1",
            }
        ]
    }

    lambda_handler(event=event_data, context=None)
