""" Modulo para interactuar con los buckets de S3. """

import os
from typing import List, Optional, Tuple, Union

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


class S3Service(metaclass=Singleton):
    """
    Clase para gestionar la conexión y operaciones con los buckets de S3.

    Esta clase implementa el patrón Singleton para garantizar que solo haya una instancia de la
    conexión al buckets de S3.
    """

    # Bandera para asegurar que la inicialización de la instancia se realice solo una vez
    _initialized: bool = False

    def __init__(self, env: Environment, logger_service: LoggerService) -> None:
        """
        Inicializa una instancia de la clase S3Service.

        """
        # Valida si ya existe una instancia
        if not self._initialized:
            # Cambia el estado de la bandera para indicar que ya existe una instancia
            self._initialized = True
            # Atributo para registrar logs
            self.logger_service: LoggerService = logger_service

            # Valida si la conexión al servicio de AWS S3 es de forma local
            if env.IS_LOCAL:
                # Inicializa un cliente para interactuar con AWS S3 local
                self.client: BaseClient = boto3.client(
                    "s3",
                    region_name=env.REGION_ZONE,
                    endpoint_url=env.LOCALSTACK_ENDPOINT,
                )
            else:
                # Inicializa un cliente para interactuar con AWS S3
                self.client: BaseClient = boto3.client(
                    "s3",
                    region_name=env.REGION_ZONE
                )

    def read_file(
            self,
            bucket: str,
            object_name: str,
            blocks: Optional[int] = 0,
    ) -> Tuple[Union[str, List[List[str]]], int, bool]:
        """
        Lee un archivo de un bucket de S3 y devuelve su contenido.


        """
        # Definición de los variables de la tupla a retornar
        result: Union[str, List[List[str]]] = "" if blocks <= 0 else []
        total_records: int = 0
        error: bool = False

        try:
            # Obtiene el objeto del bucket del S3
            obj: dict = self.client.get_object(Bucket=bucket, Key=object_name)
            # Obtiene el body del archivo del S3
            body: str = obj["Body"]

            if body and blocks > 0:
                # Variable para almacenar la información en bloques
                current_block: list = []
                for line in body.iter_lines():
                    # Agrega la linea al bloque
                    current_block.append(line.decode("utf-8"))
                    # Cuenta el número de registros
                    total_records += 1

                    # Si el bloque tiene el número de líneas especificado,
                    # se agrega a la lista de bloques
                    if len(current_block) >= blocks:
                        # Agrega el bloque al resultado final
                        result.append(current_block)
                        # Limpia la variable para almacenar la información en bloques
                        current_block = []
                # Agrega cualquier bloque restante
                if current_block:
                    result.append(current_block)
            else:
                # Lee el contenido del archivo del S3
                result = body.read()
                # Cuenta el número de líneas en el contenido completo
                total_records = len(result.splitlines())
        except (
                BotoCoreError,
                ClientError,
                EndpointConnectionError,
                NoCredentialsError,
                PartialCredentialsError,
        ) as e:
            # Cambia estado del error y obtiene la descripción del error
            error = True

        return result, total_records, error

    def download_file(
            self,
            bucket: str,
            object_name: str,
            destination_file_name: Optional[str] = None,
    ) -> bool:
        """
        Descarga un archivo de un bucket de S3.

        """
        # Define la variable del indicador de error
        error: bool = False

        # Registra log informativo de inicio de descarga del archivo del S3
        self.logger_service.log_info("Inicia descarga archivo S3")

        try:
            # Obtiene el nombre del archivo destino. Si no se envía se descarga con el nombre del
            # archivo del bucket (object_name)
            destination_file_name = destination_file_name or os.path.basename(
                object_name
            )
            # Descarga el archivo del S3
            self.client.download_file(bucket, object_name, destination_file_name)
        except (
                BotoCoreError,
                ClientError,
                EndpointConnectionError,
                NoCredentialsError,
                PartialCredentialsError,
        ) as e:
            # Cambia estado del error y obtiene la descripción del error
            error = True
            # Registra log del error al descargar el archivo del S3
            self.logger_service.log_error(
                f'Error S3 {{1}}" '
                f'"1=Error al intentar descargar el archivo {object_name} del bucket {bucket}: {e}'
            )

        return error

    def upload_file(
            self,
            file_name: str,
            bucket: str,
            object_name: Optional[str] = None,
    ) -> bool:
        """
        Carga/sube un archivo a un bucket de S3.


        """
        # Define la variable del indicador de error
        error: bool = False

        # Registra log informativo de inicio de carga del archivo al S3
        self.logger_service.log_info("Inicia carga del archivo al S3")

        try:
            # Obtiene el nombre del archivo que se va a cargar al bucket de S3
            # Si no se envía, se carga con el nombre del archivo (file_name)
            object_name = object_name or os.path.basename(file_name)
            # Carga el archivo al bucket de S3
            self.client.upload_file(file_name, bucket, object_name)
        except (
                BotoCoreError,
                ClientError,
                EndpointConnectionError,
                NoCredentialsError,
                PartialCredentialsError,
        ) as e:
            # Cambia estado del error y obtiene la descripción del error
            error = True
            # Registra log del error al cargar el archivo al S3
            self.logger_service.log_error(
                f'Error S3 {{1}}" '
                f'"1=Error al intentar cargar el archivo {object_name} al bucket {bucket}: {e}'
            )

        # Registra log informativo de fin de carga del archivo al S3
        self.logger_service.log_info("Finaliza carga del archivo al S3")

        return error

    def create_file(
            self,
            bucket: str,
            object_name: str,
            content: str,
    ) -> bool:
        """
        """
        # Define la variable del indicador de error
        error: bool = False

        # Registra log informativo de inicio de creación del archivo en S3
        self.logger_service.log_info("Inicia creacion del archivo en S3")

        try:
            # Se crea el archivo en S3 de acuerdo al contenido especifico
            self.client.put_object(Bucket=bucket, Key=object_name, Body=content)
        except (
                BotoCoreError,
                ClientError,
                EndpointConnectionError,
                NoCredentialsError,
                PartialCredentialsError,
        ) as e:
            # Cambia estado del error y obtiene la descripción del error
            error = True
            # Registra log del error al leer el archivo del S3
            self.logger_service.log_error(
                f'Error S3 {{1}}" '
                f'"1=Error al intentar crear el archivo {object_name} en el bucket {bucket}: {e}'
            )

        # Registra log informativo de fin de creación del archivo en S3
        self.logger_service.log_info("Finaliza creacion del archivo en S3")

        return error

    def move_file(
            self,
            bucket: str,
            source_object_name: str,
            destination_folder: str,
            destination_file_name: str,
    ) -> bool:
        """
        Mueve un archivo de una carpeta a otra en un bucket de S3.

        Args:
            bucket (str):
                Nombre del bucket de S3.
            source_object_name (str):
                Nombre del archivo en S3 con la ruta original.
            destination_folder (str):
                Nombre de la carpeta a la que se requiere mover el archivo.
            destination_file_name (str):
                Nombre del archivo destino.

        Returns:
            bool:
                Indicador de error.

        Raises:
            BotoCoreError:
                Si hay errores al interactuar con los buckets de S3.
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

        # Registra log informativo de inicio de operación para mover de carpeta el archivo de S3
        self.logger_service.log_info("Inicia operacion para mover de carpeta el archivo de S3")
        try:
            # Copia el archivo a la nueva ubicación
            copy_source: dict = {"Bucket": bucket, "Key": source_object_name}
            self.client.copy_object(
                CopySource=copy_source,
                Bucket=bucket,
                Key=f'{destination_folder.rstrip("/")}/{destination_file_name}',
            )
            # Elimina el archivo original
            error = self.delete_file(bucket=bucket, object_name=source_object_name)
        except (
                BotoCoreError,
                ClientError,
                EndpointConnectionError,
                NoCredentialsError,
                PartialCredentialsError,
        ) as e:
            # Cambia estado del error y obtiene la descripción del error
            error = True
            # Registra log del error al mover de carpeta el archivo de S3
            self.logger_service.log_error(
                f'Error S3 {{1}}" "1=Error al intentar mover el archivo {source_object_name} '
                f'a {destination_folder}/{destination_file_name} en el bucket {bucket}: {e}'
            )

        # Registra log informativo de fin de operación para mover de carpeta el archivo de S3
        self.logger_service.log_info("Finaliza operacion para mover de carpeta el archivo de S3")
        return error

    def delete_file(
            self,
            bucket: str,
            object_name: str,
    ) -> bool:
        """
        Elimina un archivo de un bucket de S3.

        Args:
            bucket (str):
                Nombre del bucket de S3.
            object_name (str):
                Nombre del objeto en S3.

        Returns:
            bool:
                Indicador de error.

        Raises:
            BotoCoreError:
                Si hay errores al interactuar con los buckets de S3.
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

        # Registra log informativo de inicio de eliminación del archivo del S3
        self.logger_service.log_info("Inicia eliminacion archivo S3")

        try:
            # Elimina un archivo de un bucket de S3
            error = self.client.delete_object(Bucket=bucket, Key=object_name)
        except (
                BotoCoreError,
                ClientError,
                EndpointConnectionError,
                NoCredentialsError,
                PartialCredentialsError,
        ) as e:
            # Cambia estado del error y obtiene la descripción del error
            error = True
            # Registra log del error al eliminar el archivo del S3
            self.logger_service.log_error(
                f'Error S3 {{1}}" '
                f'"1=Error al intentar eliminar el archivo {object_name} del bucket {bucket}: {e}'
            )

        # Registra log informativo de fin de eliminación del archivo del S3
        self.logger_service.log_info("Finaliza eliminacion archivo S3")

        return error
