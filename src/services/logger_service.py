""" Módulo para registrar los mensajes de los logs. """

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
    """Clase singleton para registrar los mensajes de los logs."""

    _initialized: bool = False

    def __init__(self, debug_mode: bool = False, request_id: str = None) -> None:
        if not self._initialized:
            self._initialized = True
            self.request_id = request_id or str(uuid.uuid4())
            self.context = {}
            self.logger = logging.getLogger(
                os.getenv("SERVICE_NAME", "service-name")
            )

            console_handler = StreamHandler()
            level = logging.DEBUG if debug_mode else logging.INFO

            self.logger.setLevel(level)
            console_handler.setLevel(level)
            console_handler.setFormatter(self._formatter())
            self.logger.addHandler(console_handler)

    def set_context(self, key: str, value: str) -> None:
        self.context[key] = value

    def _log(self, level: int, message: str, exc_info: bool = False) -> None:
        frame = currentframe().f_back.f_back
        module = frame.f_globals["__name__"]
        line_number = frame.f_lineno

        extra = {
            "request_id": self.request_id,
            "module_name": module,
            "line_number": line_number,
        }
        self.logger.log(level, message, extra=extra, exc_info=exc_info)

    def log_info(self, message: str) -> None:
        self._log(logging.INFO, message)

    def log_debug(self, message: str) -> None:
        self._log(logging.DEBUG, message)

    def log_error(self, message: str) -> None:
        self._log(logging.ERROR, message, exc_info=True)

    def _formatter(self) -> Formatter:
        formatter = Formatter()
        formatter.format = self._format_record
        return formatter

    def _format_record(self, record: logging.LogRecord) -> str:
        colombia_tz = timezone("America/Bogota")
        timestamp = datetime.fromtimestamp(record.created, colombia_tz)
        formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        module_name = record.__dict__.get("module_name", "")
        line_number = record.__dict__.get("line_number", "")
        filename = self.context.get("filename", "--->")

        level = f"[{record.levelname}]"
        module = f"[{module_name}:{line_number}]"
        message = record.getMessage()

        return f'{formatted_timestamp} {level} {module} [{filename}] "{message}"'
