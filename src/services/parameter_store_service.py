""" Modulo para obtener los parámetros del AWS System Manager Parameter Store. """

import json
from typing import Any, Dict, List

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

# pylint: disable=import-error
from src.services.logger_service import LoggerService
from src.utils.environment import Environment
from src.utils.singleton import Singleton


# pylint: disable=too-few-public-methods
class ParameterStoreService(metaclass=Singleton):
    """
    Clase singleton para obtener los parámetros del AWS System Manager Parameter Store.

    Esta clase garantiza que los parámetros del AWS System Manager Parameter Store se carguen
    y se validen correctamente.
    """

    # Bandera para asegurar que la inicialización de la instancia se realice solo una vez
    _initialized: bool = False

    def __init__(
        self,
        env: Environment,
        logger_service: LoggerService,
        parameter_names: List[str],
    ) -> None:
        """
        Inicializa una instancia de la clase ParameterStoreService.

        Este método se ejecuta automáticamente después de que se crea una nueva instancia de la
        clase. Se cargan y validan los parámetros del AWS System Manager Parameter Store.

        Args:
            env (Environment):
                Instancia con los valores de las variables de entorno.
            logger_service (LoggerService):
                Servicio de logging para registrar errores y eventos.
            parameter_names (List[str]):
                Nombres de los parámetros a obtener.

        Raises:
            json.JSONDecodeError:
                Si hay error al decodificar el parámetro.
            ClientError:
                Si hay error con el cliente.
            Exception:
                Si hay errores al cargar o validar el parámetro.
        """
        # Valida si ya existe una instancia
        if not self._initialized:
            # Cambia el estado de la bandera para indicar que ya existe una instancia
            self._initialized = True
            # Atributo para registrar logs
            self.logger_service: LoggerService = logger_service
            # Registra log de información para indicar que se inicia obtención de los parámetro
            self.logger_service.log_info(
                "Inicia proceso para obtener los parámetros del AWS System Manager Parameter Store"
            )

            # Valida si la conexión al servicio de AWS System Manager Parameter Store es de forma
            # local
            if env.IS_LOCAL:
                # Inicializa un cliente para interactuar con AWS System Manager Parameter Store
                # local
                self.client: BaseClient = boto3.client(
                    "ssm",
                    region_name=env.REGION_ZONE,
                    endpoint_url=env.LOCALSTACK_ENDPOINT
                )
            else:
                # Inicializa un cliente para interactuar con AWS System Manager Parameter Store
                self.client: BaseClient = boto3.client(
                    "ssm", region_name=env.REGION_ZONE
                )

            # Inicializa el diccionario de parámetros
            self.parameters: dict = {}

            # Obtiene los valores de los parámetros
            self.get_parameters(parameter_names=parameter_names)

            # Registra log de información para indicar que se finaliza la obtención de los
            # parámetros
            self.logger_service.log_info(
                "Finaliza correctamente el proceso para obtener los parámetros del AWS System"
                "Manager Parameter Store"
            )

    def get_parameters(self, parameter_names: List[str]) -> None:
        """
        Obtiene el valor de los parámetros y lo asigna como atributos de la instancia.

        Args:
            parameter_names (List[str]):
                Nombres de los parámetros a obtener.

        Raises:
            json.JSONDecodeError:
                Si hay error al decodificar el parámetro.
            ClientError:
                Si hay error con el cliente.
            Exception:
                Si hay errores al cargar o validar el parámetro.
        """
        # Recorre los nombres de los parámetros a obtener
        for parameter_name in parameter_names:
            try:
                # Obtiene el valor del parámetro
                response: Dict[str, Any] = self.client.get_parameter(
                    Name=parameter_name, WithDecryption=True
                )
                parameter_value: str = response["Parameter"]["Value"]
                # Transforma el valor del parámetro a json
                value: Dict[str, Any] = json.loads(parameter_value)
                # Asigna los valores de los parámetros como atributos de la instancia
                self._assign_parameter(parameter_name=parameter_name, value=value)
            except json.JSONDecodeError as e:
                # Registra un log de error si hay error al decodificar el parámetro y genera la
                # excepción
                self.logger_service.log_fatal(
                    f'Error al obtener los parámetros del AWS System Manager Parameter Store '
                    f'{{1}}" "1=Error al decodificar el JSON del parámetro {parameter_name}: {e}'
                )
                raise
            except ClientError as e:
                # Registra un log de error si hay errores con el cliente y genera la excepción
                self.logger_service.log_fatal(
                    f'Error al obtener los parámetros del AWS System Manager Parameter Store '
                    f'{{1}}" "1=Error de cliente obteniendo el parámetro {parameter_name}: {e}'
                )
                raise
            except Exception as e:
                # Registra un log de error si hay errores al cargar el parámetro y genera la
                # excepción
                self.logger_service.log_fatal(
                    f'Error al obtener los parámetros del AWS System Manager Parameter Store '
                    f'{{1}}" "1=Error obteniendo el valor del parámetro {parameter_name}: {e}'
                )
                raise

    def _assign_parameter(self, parameter_name: str, value: Dict[str, Any]) -> None:
        """
        Asigna el valor del parámetro a la instancia eliminando el primer segmento del path.

        Args:
            parameter_name (str):
                Nombre del parámetro a ajustar.
            value (Dict[str, Any]):
                Valor del parámetro a asignar.
        """
        # Obtiene las keys o los segmentos del parámetro, eliminando la primer llave o segmento
        keys: List[str] = parameter_name.strip("/").split("/")[1:]

        # Asigna los valores de los parámetros como atributos de la instancia
        current: dict = self.parameters
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[keys[-1]] = value
