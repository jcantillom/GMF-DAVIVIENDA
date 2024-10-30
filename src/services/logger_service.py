""" Modulo para registrar los mensajes de los logs. """

import logging
import os
import uuid
from datetime import datetime
from inspect import FrameInfo, currentframe
from logging import Formatter, Logger, StreamHandler

from pytz import timezone
from pytz.tzinfo import BaseTzInfo

# pylint: disable=import-error
from src.utils.singleton import Singleton


class LoggerService(metaclass=Singleton):
    """
    Clase singleton para registrar los mensajes de los logs.

    Servicio de logging para registrar los mensajes de los logs en formato personalizado.
    """

    # Banderas para asegurar que la inicialización de la instancia se realice solo una vez
    _initialized: bool = False

    def __init__(self, debug_mode: bool = False, request_id: str = str(uuid.uuid4())) -> None:
        """
        Inicializa una instancia de la clase LoggerService.

        Este método se ejecuta automáticamente después de que se crea una nueva instancia de la
        clase. Se encarga de inicializar el nivel de los logs, crea un administrador de consola
        para imprimir los logs en la consola, genera un formato personalizado para los logs y
        crea un id de trazabilidad.

        Args:
            debug_mode (bool):
                Variable para validar si se deben o no registrar logs de debug.
            request_id (str):
                Identificador único de la ejecución.
        """
        # Valida si ya existe una instancia
        if not self._initialized:
            # Cambia el estado de la bandera para indicar que ya existe una instancia
            self._initialized = True
            # Obtiene el valor del request id de la ejecución
            self.request_id: str = request_id
            # Nombre del log
            self.logger: Logger = logging.getLogger(
                os.getenv("SERVICE_NAME", "service-name")
            )
            # Crear un administrador de consola para imprimir logs en la consola
            console_handler: StreamHandler = logging.StreamHandler()

            # Valida si se deben registrar logs de debug
            if debug_mode:
                self.logger.setLevel(logging.DEBUG)
                console_handler.setLevel(logging.DEBUG)
            else:
                self.logger.setLevel(logging.INFO)
                console_handler.setLevel(logging.INFO)

            # Crea un formato personalizado para generar los logs
            console_handler.setFormatter(self._formatter())
            # Agregar el administrador de consola al logger
            self.logger.addHandler(console_handler)

    def log_debug(self, message: str) -> None:
        """
        Registra un mensaje de debug.

        Args:
            message (str):
                Mensaje a registrar.
        """
        self._log(level=logging.DEBUG, message=message)

    def log_info(self, message: str) -> None:
        """
        Registra un mensaje de información.

        Args:
            message (str):
                Mensaje a registrar.
        """
        self._log(level=logging.INFO, message=message)

    def log_warning(self, message: str) -> None:
        """
        Registra un mensaje de advertencia.

        Args:
            message (str):
                Mensaje a registrar.
        """
        self._log(level=logging.WARNING, message=message)

    def log_error(self, message: str) -> None:
        """
        Registra un mensaje de error con traza completa.

        Args:
            message (str):
                Mensaje a registrar.
        """
        self._log(level=logging.ERROR, message=message, exc_info=True)

    def log_fatal(self, message: str) -> None:
        """
        Registra un error fatal/bloqueante con traza completa.

        Args:
            message (str):
                Mensaje a registrar.
        """
        self._log(level=logging.FATAL, message=message, exc_info=True)

    def _log(self, level: int, message: str, exc_info: bool = False) -> None:
        """
        Registra un log con un nivel específico.

        Args:
            level (int):
                Nivel de log (DEBUG, INFO, WARNING, ERROR, FATAL(CRITICAL)).
            message (str):
                Mensaje a registrar.
            exc_info (bool):
                Información de la excepción, si la hay.
        """
        # Obtiene el módulo y la linea que generaron el log
        frame: FrameInfo = currentframe().f_back.f_back
        module: str = frame.f_globals["__name__"]
        line_number: int = frame.f_lineno

        # Genera la información extra para el log, como
        # el id de la ejecución, el módulo y la linea que generaron el log
        extra: dict = {
            "request_id": self.request_id,
            "module_name": module,
            "line_number": line_number,
        }
        # Registra el log
        self.logger.log(level, message, extra=extra, exc_info=exc_info)

    def _formatter(self) -> logging.Formatter:
        """
        Crea un formateador de logs en formato string personalizado.

        Returns:
            logging.Formatter:
                Formateador de logs.
        """
        formatter: Formatter = logging.Formatter()
        formatter.format = self._format_record
        return formatter

    @staticmethod
    def _format_record(record: logging.LogRecord) -> str:
        """
        Formatea un registro de log en un string personalizado.

        Args:
            record (logging.LogRecord):
                Registro de log.

        Returns:
            str:
                Registro de log formateado.
        """
        # Define la zona horaria de Colombia
        colombia_tz: BaseTzInfo = timezone("America/Bogota")

        # Convierte el timestamp a la zona horaria de Colombia
        timestamp: datetime = datetime.fromtimestamp(record.created, colombia_tz)

        # Formatea el timestamp
        formatted_timestamp: str = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # Obtiene los valores que se van a registrar en el log
        level: str = record.levelname
        message: str = record.getMessage()
        service_name: str = record.name
        request_id: str = record.__dict__.get("request_id", "")
        module_name: str = record.__dict__.get("module_name", "")
        line_number: int = record.__dict__.get("line_number", "")

        # Genera el formato del log personalizado
        formatted_message = (
            f'{formatted_timestamp} {level} [{module_name}:{line_number}] [{request_id}] '
            f'[{service_name}] "{message}"'
        )

        return formatted_message
