"""Modulo para interactuar con las colas de SQS."""

import json
from typing import Dict, List, Optional, Tuple, Union

import boto3
from botocore.client import BaseClient
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
    PartialCredentialsError,
)

# pylint: disable=import-error
from src.services.logger_service import LoggerService
from src.utils.environment import Environment
from src.utils.singleton import Singleton


class SQSService(metaclass=Singleton):
    """
    Clase para gestionar la conexión y operaciones con las colas de SQS.

    Esta clase implementa el patrón Singleton para garantizar que solo haya una instancia de la
    conexión a las colas de SQS.
    """

    # Bandera para asegurar que la inicialización de la instancia se realice solo una vez
    _initialized: bool = False

    def __init__(self, env: Environment, logger_service: LoggerService) -> None:
        """
        Inicializa una instancia de la clase SQSService.

        Este método se ejecuta automáticamente después de que se crea una nueva instancia de la
        clase.

        Args:
            env (Environment):
                Instancia con los valores de las variables de entorno.
            logger_service (LoggerService):
                Servicio de logging para registrar errores y eventos.

        Raises:
            BotoCoreError:
                Si hay errores al interactuar con las colas de SQS.
            ClientError:
                Si hay error con el cliente.
            EndpointConnectionError:
                Si hay error de conexión al endpoint.
            NoCredentialsError:
                Si hay error de credenciales.
            PartialCredentialsError:
                Si hay error de credenciales incompletas.
        """
        # Valida si ya existe una instancia
        if not self._initialized:
            # Cambia el estado de la bandera para indicar que ya existe una instancia
            self._initialized = True
            # Atributo para registrar logs
            self.logger_service: LoggerService = logger_service

            # Valida si la conexión al servicio de AWS SQS es de forma local
            if env.IS_LOCAL:
                # Inicializa un cliente para interactuar con AWS SQS local
                self.client: BaseClient = boto3.client(
                    "sqs",
                    region_name=env.REGION_ZONE,
                    endpoint_url=env.LOCALSTACK_ENDPOINT,
                )
            else:
                # Inicializa un cliente para interactuar con AWS SQS
                self.client: BaseClient = boto3.client(
                    "sqs",
                    region_name=env.REGION_ZONE,
                )

    def get_messages(
        self,
        queue_url: str,
        max_messages: Optional[int] = 1,
        wait_time_seconds: Optional[int] = 0,
    ) -> Tuple[List[Dict], bool]:
        """
        Obtiene mensajes de una cola de SQS.

        Args:
            queue_url (str):
                URL de la cola de SQS.
            max_messages (Optional[int]):
                Máximo número de mensajes a obtener (opcional).
                Por defecto se obtiene solo un mensaje.
            wait_time_seconds (Optional[int]):
                Tiempo de espera para obtener mensajes nuevos antes de que la solicitud
                expire (opcional). Por defecto 0, no espera mensajes nuevos.

        Returns:
            Tuple[List[Dict] bool, str]:
                Mensajes de la cola obtenidos e indicador de error.

        Raises:
            BotoCoreError:
                Si hay errores al interactuar con las colas de SQS.
            ClientError:
                Si hay error con el cliente.
            EndpointConnectionError:
                Si hay error de conexión al endpoint.
            NoCredentialsError:
                Si hay error de credenciales.
            PartialCredentialsError:
                Si hay error de credenciales incompletas.
        """
        # Definición de los variables de la tupla a retornar
        result: List[Dict] = []
        error: bool = False

        # Registra log informativo de inicio de obtención de mensajes del SQS
        self.logger_service.log_info("Inicia obtencion de mensajes del SQS")

        try:
            # Obtiene mensajes de la cola de SQS
            messages: dict = self.client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time_seconds,
            )
            result = messages.get("Messages", [])
        except (
            BotoCoreError,
            ClientError,
            EndpointConnectionError,
            NoCredentialsError,
            PartialCredentialsError,
        ) as e:
            # Cambia estado del error y obtiene la descripción del error
            error = True
            # Registra log del error al obtener los mensajes del SQS
            self.logger_service.log_error(
                f'Error SQS {{1}}" '
                f'"1=Error al intentar obtener los mensajes de la cola de SQS {queue_url}: {e}'
            )

        # Registra log informativo de fin de obtención de mensajes del SQS
        self.logger_service.log_info("Finaliza obtencion de mensajes del SQS")

        return result, error

    def send_message(
        self,
        queue_url: str,
        message_body: Union[str, dict],
        delay_seconds: Optional[int] = 0,
    ) -> bool:
        """
        Envía un mensaje a una cola de SQS.

        Args:
            queue_url (str):
                URL de la cola de SQS.
            message_body (Union[str, dict]):
                Cuerpo del mensaje.
            delay_seconds (Optional[int]):
                Segundos de retraso para que el mensaje no esté disponible para ser recibido por
                los consumidores (opcional, valor máximo 900 segundos (15 minutos)).

        Returns:
            bool:
                Indicador de error.

        Raises:
            BotoCoreError:
                Si hay errores al interactuar con las colas de SQS.
            ClientError:
                Si hay error con el cliente.
            EndpointConnectionError:
                Si hay error de conexión al endpoint.
            NoCredentialsError:
                Si hay error de credenciales.
            PartialCredentialsError:
                Si hay error de credenciales incompletas.
        """
        # Define la variable del indicador de error
        error: bool = False

        # Registra log informativo de inicio de envío de mensajes al SQS
        self.logger_service.log_info("Inicia envio de mensajes al SQS")

        try:
            # Convierte el mensaje a cadena JSON si es un diccionario
            if isinstance(message_body, dict):
                message_body = json.dumps(message_body, ensure_ascii=False)

            # Envía el mensaje a la cola de SQS
            self.client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
                DelaySeconds=delay_seconds,
            )
        except (
            BotoCoreError,
            ClientError,
            EndpointConnectionError,
            NoCredentialsError,
            PartialCredentialsError,
        ) as e:
            # Cambia estado del error y obtiene la descripción del error
            error = True
            # Registra log del error al enviar los mensajes al SQS
            self.logger_service.log_error(
                f'Error SQS {{1}}" '
                f'"1=Error al intentar enviar los mensajes a la cola de SQS {queue_url}: {e}'
            )

        # Registra log informativo de fin de envío de mensajes al SQS
        self.logger_service.log_info("Finaliza envio de mensajes al SQS")

        return error

    def delete_message(
        self,
        queue_url: str,
        receipt_handle: str,
    ) -> bool:
        """
        Elimina un mensaje de una cola de SQS.

        Args:
            queue_url (str):
                URL de la cola de SQS.
            receipt_handle (str):
                Receipt handle del mensaje a eliminar.

        Returns:
            bool:
                Indicador de error.

        Raises:
            BotoCoreError:
                Si hay errores al interactuar con las colas de SQS.
            ClientError:
                Si hay error con el cliente.
            EndpointConnectionError:
                Si hay error de conexión al endpoint.
            NoCredentialsError:
                Si hay error de credenciales.
            PartialCredentialsError:
                Si hay error de credenciales incompletas.
        """
        # Define la variable del indicador de error
        error: bool = False

        # Registra log informativo de inicio de eliminación de mensajes del SQS
        self.logger_service.log_info("Inicia eliminacion de mensajes del SQS")

        try:
            # Elimina el mensaje de la cola de SQS
            self.client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle,
            )
        except (
            BotoCoreError,
            ClientError,
            EndpointConnectionError,
            NoCredentialsError,
            PartialCredentialsError,
        ) as e:
            # Cambia estado del error y obtiene la descripción del error
            error = True
            # Registra log del error al eliminar los mensajes del SQS
            self.logger_service.log_error(
                f'Error SQS {{1}}" "1=Error al intentar eliminar el mensaje ({receipt_handle}) '
                f'de la cola de SQS {queue_url}: {e}'
            )

        # Registra log informativo de fin de eliminación de mensajes del SQS
        self.logger_service.log_info("Finaliza eliminacion de mensajes del SQS")

        return error
