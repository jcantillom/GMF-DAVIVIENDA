import logging
from unittest import TestCase
from unittest.mock import MagicMock, patch

# Service
from src.services.logger_service import LoggerService


class TestLoggerService(TestCase):
    """Clase para el manejo de tests de LoggerService"""

    def setUp(self):
        self.logger_service = LoggerService(debug_mode=True)

    @patch.object(LoggerService, "_log")
    def test_log_functions(self, mock_log: MagicMock) -> None:
        """Test para validar el correcto funcionamiento de log_debug."""
        expected_value = "test"
        mock_log.return_value = None

        # Validación de log_debug
        self.logger_service.log_debug(expected_value)
        mock_log.assert_called_with(level=logging.DEBUG, message=expected_value)

        # Validación de log_info
        self.logger_service.log_info(expected_value)
        mock_log.assert_called_with(level=logging.INFO, message=expected_value)

        # Validación de log_warning
        self.logger_service.log_warning(expected_value)
        mock_log.assert_called_with(level=logging.WARNING, message=expected_value)

        # Validación de log_error
        self.logger_service.log_error(expected_value)
        mock_log.assert_called_with(
            level=logging.ERROR, message=expected_value, exc_info=True
        )

        # Validación de log_fatal
        self.logger_service.log_fatal(expected_value)
        mock_log.assert_called_with(
            level=logging.FATAL, message=expected_value, exc_info=True
        )

    @patch.object(LoggerService, "_log")
    def test_log_functions_without_exception(self, mock_log: MagicMock) -> None:
        """Test para validar el correcto funcionamiento de log_error y log_fatal sin excepción."""
        expected_value = "test"
        mock_log.return_value = None

        # Validación de log_error
        self.logger_service.log_error(expected_value)
        mock_log.assert_called_with(
            level=logging.ERROR, message=expected_value, exc_info=True
        )

        # Validación de log_fatal
        self.logger_service.log_fatal(expected_value)
        mock_log.assert_called_with(
            level=logging.FATAL, message=expected_value, exc_info=True
        )

    @patch.object(LoggerService, "_log")
    def test_log_functions_with_exception(self, mock_log: MagicMock) -> None:
        """Test para validar el correcto funcionamiento de log_error y log_fatal con excepción."""
        expected_value = "test"
        mock_log.side_effect = Exception

        # Validación de log_error
        with self.assertRaises(Exception):
            self.logger_service.log_error(expected_value)

        # Validación de log_fatal
        with self.assertRaises(Exception):
            self.logger_service.log_fatal(expected_value)


    def test_singleton_behavior(self):
        """Validar que la clase LoggerService siga el patrón Singleton."""
        logger_instance_1 = LoggerService()
        logger_instance_2 = LoggerService()

        # Ambas instancias deben ser la misma
        self.assertIs(logger_instance_1, logger_instance_2)

    @patch("logging.StreamHandler.emit")
    def test_log_output_format(self, mock_emit: MagicMock):
        """Validar que el mensaje del log se formatee correctamente."""
        self.logger_service.log_info("Testing log format")

        # Obtener el registro de log capturado y el mensaje formateado
        log_record = mock_emit.call_args[0][0]  # Obtiene el registro de log emitido
        formatted_message = self.logger_service._format_record(log_record)

        # Validar el contenido del mensaje de log
        self.assertIn("INFO", formatted_message)
        self.assertIn("Testing log format", formatted_message)

if __name__ == "__main__":
    unittest.main()
