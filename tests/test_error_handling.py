import unittest
from unittest.mock import MagicMock, patch, Mock
from src.core.error_handling import ErrorHandling
from src.services.database_service import DatabaseService
from src.services.logger_service import LoggerService
from src.services.sqs_service import SQSService
from src.services.s3_service import S3Service
from src.utils.environment import Environment
from datetime import datetime
from src.models.cgd_archivos import CGDArchivos
from src.models.cgd_catalogo_errores import CGDCatalogoErrores
from unittest.mock import ANY
from src.utils.datetime_management import DatetimeManagement
from src.core.format_name_file import extract_string_after_slash
import sys

class TestErrorHandling(unittest.TestCase):

    def setUp(self):
        # Mock de los servicios
        self.mock_logger_service = MagicMock(LoggerService)
        self.mock_postgres_service = MagicMock(DatabaseService)
        self.mock_sqs_service = MagicMock(SQSService)
        self.mock_s3_service = MagicMock(S3Service)
        self.mock_env = MagicMock(Environment)

        # Mock de las variables de entorno
        self.mock_env.FAILED_FILES_FOLDER = "failed-files"
        self.mock_env.FOLDER_REJECTED = "rejected-files"
        self.mock_env.PROCESSED_FILES_FOLDER = "processed-files"
        self.mock_env.SQS_URL_EMAILS = "email-queue"
        self.mock_env.QUEUE_URL_SOURCE = "source-queue"
        self.mock_env.QUEUE_URL_DESTINATION = "destination-queue"
        self.mock_env.BUCKET = "test-bucket"
        self.mock_env.AWS_BUCKET_CARPETA_RECHAZADOS = "rejected-files"
        self.mock_env.QUEUE_URL_EMAILS = "email-sqs-url"
        self.mock_env.SQS_URL_PRO_RESPONSE_TO_PROCESS = "process-sqs-url"
        # Mock de los parámetros de la tienda de parámetros
        self.mock_parameter_store_service = MagicMock()
        self.mock_parameter_store_service.parameters = {
            "transversal": {
                "config-retries": {
                    "number-retries": 3,
                    "time-between-retry": 5
                }
            }
        }

        # Datos del evento
        self.event_data = {
            "bucket": "test-bucket",
            "path_file_name": "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip",
            "file_name": "Recibidos/RE_PRO_TUTGMF0001003920240930-0001",
            "extension": ".zip",
            "timestamp": "1691318400000",
            "sqs_receipt_handle": "receipt-handle",
            "date": "2023-10-06",
            "time": "12:00",
            "sqs_message": {
                "Records": [
                    {
                        "body": "message-body"
                    }
                ]
            }
        }

        # Diccionario de servicios
        self.services = {
            "env": self.mock_env,
            "logger_service": self.mock_logger_service,
            "postgres_service": self.mock_postgres_service,
            "sqs_service": self.mock_sqs_service,
            "s3_service": self.mock_s3_service,
            "parameter_store_service": self.mock_parameter_store_service
        }

        # Instancia de ErrorHandling
        self.error_handling = ErrorHandling(self.services, self.event_data)

    # Pruebas para send_notification
    def test_send_notification(self):
        values = {"id_plantilla": "PC001", "codigo_rechazo": "001"}
        self.mock_postgres_service.get_by_id.return_value = [{"descripcion": "Error test"}]

        with patch.object(self.error_handling, 'finish_process') as mock_finish_process:
            self.error_handling.send_notification(values, move_file=True, error=False)

            # Verifica que se llame a SQS para enviar un mensaje de correo
            self.mock_sqs_service.send_message.assert_called_once()
            mock_finish_process.assert_called_once_with(error=False, move_file=True)

    def test_send_notification_no_description(self):
        values = {"id_plantilla": "PC001", "codigo_rechazo": "001"}
        self.mock_postgres_service.get_by_id.return_value = [{"descripcion": "Error test"}]

        with patch.object(self.error_handling, 'finish_process') as mock_finish_process:
            self.error_handling.send_notification(values, move_file=False, error=True)

            # Verifica que se llamó a get_by_id para obtener la descripción
            self.mock_postgres_service.get_by_id.assert_called_with(
                model=CGDCatalogoErrores,
                columns=[CGDCatalogoErrores.descripcion],
                record_id=values["codigo_rechazo"],
                id_name="codigo_error"
            )

            mock_finish_process.assert_called_once_with(error=True, move_file=False)

    # Pruebas para finish_process
    def test_finish_process_success(self):
        with patch('sys.exit') as mock_exit:
            destination_message = {"key": "value"}
            self.error_handling.finish_process(error=False, move_file=True, destination_message=destination_message)

            self.mock_sqs_service.send_message.assert_called_once_with(
                queue_url=self.mock_env.QUEUE_URL_DESTINATION,
                message_body=destination_message
            )
            self.mock_s3_service.move_file.assert_called_once()
            self.mock_sqs_service.delete_message.assert_called_once()
            mock_exit.assert_called_once()

    def test_finish_process_with_error(self):
        with patch('sys.exit') as mock_exit:
            self.error_handling.finish_process(error=True, move_file=False)

            # Verifica que no se llamó a send_message para la cola de destino
            self.mock_sqs_service.send_message.assert_not_called()

            # Verifica que no se movió el archivo
            self.mock_s3_service.move_file.assert_not_called()

            # Verifica que se eliminó el mensaje de la cola fuente
            self.mock_sqs_service.delete_message.assert_called_once_with(
                queue_url=self.mock_env.QUEUE_URL_SOURCE,
                receipt_handle=self.event_data["sqs_receipt_handle"]
            )

            # Verifica que se llamó a sys.exit()
            mock_exit.assert_called_once()
   
    @patch('src.utils.datetime_management.DatetimeManagement')
    def test_process_file_error_reprocess_not_applicable(self, mock_datetime_management):
        # Arrange
        updates = {"error_code": "EICP002", "final_state": "FALLIDO"}
        file_id = 1

        # Mock timestamp methods
        mock_datetime_management.get_datetime.return_value = {"timestamp": "20231007120000.000"}
        mock_datetime_management.convert_string_to_date.return_value = datetime(2023, 10, 7, 12, 0)

        # Mock database responses
        self.mock_postgres_service.get_by_id.side_effect = [
            # First call returns file info with retry count equal to max retries
            [{"estado": "INICIAL", "contador_intentos_cargue": 3}],
            # Second call returns error info indicating reprocessing is not applicable
            [{"descripcion": "Error description", "aplica_reprogramar": False}]
        ]

        # Act
        with patch.object(self.error_handling, 'send_notification') as mock_send_notification:
            self.error_handling.process_file_error(updates, file_id)

            # Assert
            # Check that send_notification was called with move_file=True
            mock_send_notification.assert_called_once_with(
                values=ANY,
                description_rejection="Error description",
                move_file=True,
            )

            # Verify no message sent back to source queue
            self.mock_sqs_service.send_message.assert_not_called()

            # Verify database update
            self.mock_postgres_service.update_by_id.assert_called_once_with(
                model=CGDArchivos,
                record_id=file_id,
                id_name="id_archivo",
                updates={
                    "estado": "FALLIDO",
                    "codigo_error": "EICP002",
                    "detalle_error": "Error description"
                }
            )

            # Verify state change logged
            self.mock_postgres_service.insert.assert_called_once()
            
    @patch('src.core.error_handling.DatetimeManagement')
    def test_process_file_error_exception_during_insert(self, mock_datetime_management):
        # Arrange
        updates = {"error_code": "EICP004", "final_state": "FALLIDO"}  # Añadido 'final_state'
        file_id = 1

        # Mock timestamp methods
        mock_datetime_management.get_datetime.return_value = {"timestamp": "20231007120000.000"}
        mock_datetime_management.convert_string_to_date.return_value = datetime(2023, 10, 7, 12, 0)

        # Mock database responses
        self.mock_postgres_service.get_by_id.side_effect = [
            [{"estado": "INICIAL", "contador_intentos_cargue": 1}],
            [{"descripcion": "Error description", "aplica_reprogramar": False}]
        ]

        # Simulate exception during insert
        self.mock_postgres_service.insert.side_effect = Exception("Database insert failed")

        # Act & Assert
        with self.assertRaises(Exception) as context:
            self.error_handling.process_file_error(updates, file_id)

        self.assertEqual(str(context.exception), "Database insert failed")

        # Verify that send_notification was not called
        self.mock_sqs_service.send_message.assert_not_called()


    @patch('src.core.error_handling.sys.exit')
    @patch('src.core.error_handling.extract_string_after_slash')
    @patch.object(ErrorHandling, '_message_structure')
    def test_errors_success(self, mock_message_structure, mock_extract_string, mock_sys_exit):
        # Arrange
        file_data = {
            "codigo_error": "E001",
            "file_name": "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip",
            "estado": "INICIAL"
        }

        # Mock de extract_string_after_slash
        mock_extract_string.return_value = "RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de get_all para obtener el código y descripción del error
        self.mock_postgres_service.get_all.return_value = (
            [{"codigo_error": "E001", "descripcion": "Descripción del error"}], None, None
        )

        # Mock de s3_service.move_file para que retorne True
        self.mock_s3_service.move_file.return_value = True

        # Mock de _message_structure
        mock_message_structure.return_value = {"mensaje": "estructura"}

        # Mock de sqs_service.send_message
        self.mock_sqs_service.send_message.return_value = None

        # Mock de sqs_service.get_messages
        self.mock_sqs_service.get_messages.return_value = (
            [{"ReceiptHandle": "receipt-handle"}], None
        )

        # Mock de sqs_service.delete_message
        self.mock_sqs_service.delete_message.return_value = True

        # Act
        self.error_handling.errors(file_data)

        # Assert
        # Verificar que se llamaron los métodos esperados
        self.mock_logger_service.log_info.assert_any_call("Comienza el flujo de error")
        self.mock_logger_service.log_info.assert_any_call(
            "Archivo movido a la carpeta rechazados/RE_PRO_TUTGMF0001003920240930-0001.zip"
        )
        self.mock_logger_service.log_info.assert_any_call("Inicio envío mensaje cola 'Envío de correos'")
        self.mock_logger_service.log_info.assert_any_call("Fin envío mensaje cola 'Envío de correos'")
        self.mock_logger_service.log_info.assert_any_call("Fin eliminación mensaje de cola")
        self.mock_logger_service.log_info.assert_any_call(
            "FIN PROCESO MANEJO DE ERRORES PARA ESTADO: INICIAL"
        )
        mock_sys_exit.assert_called_once_with(0)

    @patch('src.core.error_handling.extract_string_after_slash')
    def test_errors_move_file_failure(self, mock_extract_string):
        # Arrange
        file_data = {
            "codigo_error": "E001",
            "file_name": "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip",
            "estado": "INICIAL"
        }

        mock_extract_string.return_value = "RE_PRO_TUTGMF0001003920240930-0001.zip"

        self.mock_postgres_service.get_all.return_value = (
            [{"codigo_error": "E001", "descripcion": "Descripción del error"}], None, None
        )

        self.mock_s3_service.move_file.return_value = False

        # Act
        result = self.error_handling.errors(file_data)

        # Assert
        self.mock_logger_service.log_error.assert_called_with(
            "No se pudo mover a la carpeta rechazados"
        )
        self.assertIsNone(result)

    @patch('src.core.error_handling.extract_string_after_slash')
    def test_errors_get_messages_failure(self, mock_extract_string):
        # Arrange
        file_data = {
            "codigo_error": "E002",
            "file_name": "Recibidos/archivo_error.zip",
            "estado": "INICIAL"
        }

        mock_extract_string.return_value = "archivo_error.zip"

        self.mock_postgres_service.get_all.return_value = (
            [{"codigo_error": "E002", "descripcion": "Descripción del error"}], None, None
        )

        self.mock_s3_service.move_file.return_value = True

        # Simular error al obtener mensajes de SQS
        self.mock_sqs_service.get_messages.return_value = (None, True)

        # Act
        result = self.error_handling.errors(file_data)

        # Assert
        self.mock_logger_service.log_error.assert_called_with(
           "Error 'id_parametro'"
        )
        self.assertIsNone(result)

    @patch('src.core.error_handling.extract_string_after_slash')
    def test_errors_no_messages_in_sqs(self, mock_extract_string):
        # Arrange
        file_data = {
            "codigo_error": "E003",
            "file_name": "Recibidos/archivo_sin_mensajes.zip",
            "estado": "INICIAL"
        }

        mock_extract_string.return_value = "archivo_sin_mensajes.zip"

        self.mock_postgres_service.get_all.return_value = (
            [{"codigo_error": "E003", "descripcion": "Descripción del error"}], None, None
        )

        self.mock_s3_service.move_file.return_value = True

        # Simular que no se encuentran mensajes
        self.mock_sqs_service.get_messages.return_value = ([], None)

        # Act
        result = self.error_handling.errors(file_data)

        # Assert
        self.mock_logger_service.log_error.assert_called_with(
            "Error 'id_parametro'"
        )
        self.assertIsNone(result)

    @patch('src.core.error_handling.extract_string_after_slash')
    def test_errors_receipt_handle_none(self, mock_extract_string):
        # Arrange
        file_data = {
            "codigo_error": "E004",
            "file_name": "Recibidos/archivo_sin_receipt.zip",
            "estado": "INICIAL"
        }

        mock_extract_string.return_value = "archivo_sin_receipt.zip"

        self.mock_postgres_service.get_all.return_value = (
            [{"codigo_error": "E004", "descripcion": "Descripción del error"}], None, None
        )

        self.mock_s3_service.move_file.return_value = True

        # Simular mensaje sin ReceiptHandle
        self.mock_sqs_service.get_messages.return_value = ([{}], None)

        # Act
        result = self.error_handling.errors(file_data)

        # Assert
        self.mock_logger_service.log_error.assert_called_with(
           "Error 'id_parametro'"
        )
        self.assertIsNone(result)

    @patch('src.core.error_handling.extract_string_after_slash')
    def test_errors_delete_message_failure(self, mock_extract_string):
        # Arrange
        file_data = {
            "codigo_error": "E005",
            "file_name": "Recibidos/archivo_no_elimina.zip",
            "estado": "INICIAL"
        }

        mock_extract_string.return_value = "archivo_no_elimina.zip"

        self.mock_postgres_service.get_all.return_value = (
            [{"codigo_error": "E005", "descripcion": "Descripción del error"}], None, None
        )

        self.mock_s3_service.move_file.return_value = True

        self.mock_sqs_service.get_messages.return_value = (
            [{"ReceiptHandle": "receipt-handle"}], None
        )

        # Simular fallo al eliminar el mensaje
        self.mock_sqs_service.delete_message.return_value = False

        # Act
        result = self.error_handling.errors(file_data)

        # Assert
        self.mock_logger_service.log_error.assert_called_with(
           "Error 'id_parametro'"
        )
        self.assertIsNone(result)

if __name__ == "__main__":
    unittest.main()
