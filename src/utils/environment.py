""" Modulo para gestionar las variables de entorno. """

# Dependencias
import json
import os
from typing import Dict, Type, Union

# Dependencias externas
from dotenv import load_dotenv

# pylint: disable=import-error
# Services
from src.services.logger_service import LoggerService

# Utils
from src.utils.singleton import Singleton


# pylint: disable=too-few-public-methods
class Environment(metaclass=Singleton):
    """
    Clase singleton para gestionar variables de entorno.

    Esta clase garantiza que las variables de entorno se carguen y se validen correctamente.
    """

    # Bandera para asegurar que la inicialización de la instancia se realice solo una vez
    _initialized: bool = False

    def __init__(self, logger_service: LoggerService, expected_vars: Dict[str, Type]) -> None:
        """
        Inicializa una instancia de la clase Environment.

        Este método se ejecuta automáticamente después de que se crea una nueva instancia de la
        clase. Aquí se carga y valida las variables de entorno y se inicializan las variables
        esperadas y otros atributos de la instancia.

        Args:
            logger_service (LoggerService):
                Servicio de logging para registrar errores y eventos.
            expected_vars (Dict[str, Type]):
                Diccionario con las variables de entorno esperadas y sus tipos de datos.

        Raises:
            EnvironmentError:
                Si hay errores al cargar o validar las variables de entorno.
        """
        # Valida si ya existe una instancia
        if not self._initialized:
            # Cambia el estado de la bandera para indicar que ya existe una instancia
            self._initialized = True
            # Carga las variables de entorno desde un archivo .env si existe
            load_dotenv()
            # Atributo para registrar logs
            self.logger_service: LoggerService = logger_service
            # Registra log de debug para indicar que se inicia cargue de las variables de entorno
            self.logger_service.log_debug("Inicia cargue de las variables de entorno")
            # Obtiene las variables de entorno esperadas y sus tipos de datos
            self.expected_vars: Dict[str, Type] = expected_vars
            # Bandera para validar si se generaron errores obteniendo las variables de entorno
            self.error: bool = False
            # Inicializa y valida las variables de entorno
            self._load_env_variables()
            # Registra log de debug para indicar que las variables se cargaron correctamente
            self.logger_service.log_debug(
                "Finaliza correctamente el cargue de las variables de entorno"
            )

    def _load_env_variables(self) -> None:
        """
        Carga y valida las variables de entorno.

        Este método lee las variables de entorno definidas en self.expected_vars, las convierte al
        tipo apropiado y las establece como atributos de la instancia. Si falta alguna variable o
        es inválida, se registra un error y se lanza un EnvironmentError.

        Raises:
            EnvironmentError:
                Si hay errores al cargar o validar las variables de entorno.
        """
        # Recorre las variables de entorno esperadas
        for var, var_type in self.expected_vars.items():
            # Obtiene el valor de la variable de entorno
            value: Union[str, None] = os.getenv(var)

            if value is None:
                # Registra log de error si la variable de entorno no está configurada
                self.logger_service.log_fatal(
                    f'Error al cargar las variables de entorno {{1}}" '
                    f'"1=La variable de entorno {var} no se encuentra configurada'
                )
                self.error = True
            else:
                try:
                    # Convierte el valor de la variable de entorno al tipo apropiado
                    converted_value: Union[str, int, bool, dict, list] = (
                        self._convert_type(value=value, var_type=var_type)
                    )
                    # Establece los valores de las variables de entorno como atributos
                    setattr(self, var, converted_value)
                except ValueError as e:
                    # Registra log de error si la conversión falla
                    self.logger_service.log_fatal(
                        f'Error al cargar las variables de entorno {{1}}" '
                        f'"1=Error al convertir la variable de entorno {var}: {e}'
                    )
                    self.error = True

        if self.error:
            # Lanza un EnvironmentError y finaliza la ejecución
            raise EnvironmentError(
                "Error en la configuración de las variables de entorno"
            )

    @staticmethod
    def _convert_type(value: str, var_type: Type) -> Union[str, int, bool, dict, list]:
        """
        Convierte la variable de entorno al tipo especificado.

        Args:
            value (str):
                Valor de la variable de entorno.
            var_type (Type):
                Tipo de dato al cual se debe convertir el valor.

        Returns:
            Union[str, int, bool, dict, list]:
                Valor convertido al tipo especificado.

        Raises:
            ValueError:
                Si el valor no se puede convertir al tipo especificado.
            json.JSONDecodeError:
                Si el valor no se puede decodificar cuando el tipo es dict o list.
        """
        # Convierte las variables de tipo booleano
        if var_type == bool:
            if value.lower() in ["true", "false"]:
                return value.lower() == "true"
            raise ValueError(f'invalid value for boolean: "{value}"')
        # Convierte las variables de tipo integer
        if var_type == int:
            return int(value)
        # Convierte las variables de tipo dictionary o lists
        if var_type in [dict, list]:
            return json.loads(value)
        # Retorna el valor en string
        return value
