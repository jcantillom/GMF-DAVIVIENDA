""" Modulo para obtener los secrets de AWS. """

import json
from typing import Any, Dict, List, Union

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

# pylint: disable=import-error
from src.services.logger_service import LoggerService
from src.utils.environment import Environment
from src.utils.singleton import Singleton


# pylint: disable=too-few-public-methods
class SecretsService(metaclass=Singleton):
    """
    Clase singleton para obtener los secrets de AWS Secrets Manager.

    Esta clase garantiza que los secrets de AWS Secrets Manager se carguen y se validen
    correctamente.
    """

    # Bandera para asegurar que la inicialización de la instancia se realice solo una vez
    _initialized: bool = False

    def __init__(
        self,
        env: Environment,
        logger_service: LoggerService,
        secret_name: str,
        keys_secrets: List[str],
    ) -> None:
        """
        Inicializa una instancia de la clase SecretsService.

        Este método se ejecuta automáticamente después de que se crea una nueva instancia de la
        clase. Se cargan y validan los secrets del AWS Secrets Manager.

        Args:
            env (Environment):
                Instancia con los valores de las variables de entorno.
            logger_service (LoggerService):
                Servicio de logging para registrar errores y eventos.
            secret_name (str):
                Nombre del secret.
            keys_secrets (List[str]):
                Llaves con los nombres de los valores que se requieren obtener.

        Raises:
            ValueError:
                Si no se puede obtener el valor del secret.
            ClientError:
                Si hay error con el cliente.
            json.JSONDecodeError:
                Si hay error al decodificar el secret.
            Exception:
                Si hay errores al cargar o validar el secret.
        """
        # Valida si ya existe una instancia
        if not self._initialized:
            # Cambia el estado de la bandera para indicar que ya existe una instancia
            self._initialized = True
            # Atributo para registrar logs
            self.logger_service: LoggerService = logger_service
            # Registra log de información para indicar que se inicia obtención de los secrets
            self.logger_service.log_info(
                "Inicia proceso para obtener los secrets de AWS Secrets Manager"
            )

            # Valida si la conexión al servicio de AWS Secrets Manager es de forma local
            if env.IS_LOCAL:
                # Inicializa un cliente para interactuar con AWS Secrets Manager local
                self.client: BaseClient = boto3.client(
                    "secretsmanager",
                    region_name=env.REGION_ZONE,
                    endpoint_url=env.LOCALSTACK_ENDPOINT,
                )
            else:
                # Inicializa un cliente para interactuar con AWS Secrets Manager
                self.client: BaseClient = boto3.client(
                    "secretsmanager",
                    region_name=env.REGION_ZONE,
                )

            # Obtiene los valores de los secrets
            self._get_secret_value(secret_name=secret_name, keys_secrets=keys_secrets)

            # Registra log de información para indicar que se finaliza la obtención de los secrets
            self.logger_service.log_info(
                "Finaliza correctamente el proceso para obtener los secrets de AWS Secrets Manager"
            )

    def _get_secret_value(self, secret_name: str, keys_secrets: List[str]) -> None:
        """
        Obtiene el valor de un secret específico y lo asigna como atributos de la instancia.

        Args:
            secret_name (str):
                Nombre del secret.
            keys_secrets (List[str]):
                Llaves con los nombres de los valores que se requieren obtener.

        Raises:
            ValueError:
                Si no se puede obtener el valor del secret.
            json.JSONDecodeError:
                Si hay error al decodificar el secret.
            ClientError:
                Si hay error con el cliente.
            Exception:
                Si hay errores al cargar o validar el secret.
        """
        try:
            # Obtiene el valor de un secret específico
            get_secret_value: Dict[str, Any] = self.client.get_secret_value(
                SecretId=secret_name
            )
            secret_string: Union[str, None] = get_secret_value.get("SecretString")

            # Valida si pudo obtener el valor del secret
            if secret_string:
                # Transforma el valor del secret a json
                secrets: Dict[str, Any] = json.loads(secret_string)
                # Asigna los valores de los secrets como atributos de la instancia
                for key in keys_secrets:
                    if key in secrets:
                        setattr(self, key, secrets[key])
                    else:
                        # Genera un error si no encuentra las claves en los secret
                        raise ValueError(
                            f'Error al obtener los secrets del AWS Secrets Manager {{1}}" '
                            f'"1=La clave {key} no se encuentra en el secret {secret_name}'
                        )
            else:
                # Genera un error si no encuentra el secret
                raise ValueError(
                    f'Error al obtener los secrets del AWS Secrets Manager {{1}}" '
                    f'"1=No se pudo obtener el valor del secret {secret_name}'
                )

        except json.JSONDecodeError as e:
            # Registra un log de error si hay error al decodificar el secret y genera la excepción
            self.logger_service.log_fatal(
                f'Error al obtener los secrets del AWS Secrets Manager {{1}}" '
                f'"1=Error al decodificar el JSON del secret {secret_name}: {e}'
            )
            raise
        except ValueError as e:
            # Registra un log un error si falla la obtención de los datos y genera la excepción
            self.logger_service.log_fatal(e)
            raise
        except ClientError as e:
            # Registra un log de error si hay errores con el cliente y genera la excepción
            self.logger_service.log_fatal(
                f'Error al obtener los secrets del AWS Secrets Manager {{1}}" '
                f'"1=Error de cliente obteniendo el secret {secret_name}: {e}'
            )
            raise
        except Exception as e:
            # Registra un log de error si hay errores al cargar el secret y genera la excepción
            self.logger_service.log_fatal(
                f'Error al obtener los secrets del AWS Secrets Manager {{1}}" '
                f'"1=Error obteniendo el valor del secret {secret_name}: {e}'
            )
            raise
