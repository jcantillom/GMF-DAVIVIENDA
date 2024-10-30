from unittest import TestCase
from unittest.mock import MagicMock
from botocore.exceptions import BotoCoreError
from src.services.sqs_service import SQSService
from src.services.logger_service import LoggerService
from src.utils.environment import Environment

class TestSqsService(TestCase):
    """Clase para el manejo de test de SQSService"""

    def setUp(self):
        # Mock del Environment y LoggerService
        self.mock_env = MagicMock(spec=Environment)
        self.mock_env.IS_LOCAL = True
        self.mock_env.REGION_ZONE = 'us-east-1'
        self.mock_env.LOCALSTACK_ENDPOINT = 'http://localhost:4566'

        self.mock_logger_service = MagicMock(spec=LoggerService)

        # Instanciar SQSService con los mocks
        self.sqs_service = SQSService(env=self.mock_env, logger_service=self.mock_logger_service)
        # Mock del cliente de SQS
        self.sqs_service.client = MagicMock()

    def test_get_messages(self) -> None:
        """Test para la función get_messages - success."""
        # Valores mockeados
        queue_url = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/files-to-packaged"
        max_messages = 1
        wait_time_seconds = 1
        self.sqs_service.client.receive_message.return_value = {
            "Messages": [
                {
                    "MessageId": "d9c3a65e-eed0-482e-b09b-11ffec236618",
                    "ReceiptHandle": "ZmZiOTUzMjktMzEyMy00NDMzLWI1NjItNjQ3YjhhZmZjZmM5IGFybjphd3M6c3FzOnVzLWVhc3QtMTowMDAwMDAwMDAwMDA6Z21mLXNxcyBkOWMzYTY1ZS1lZWQwLTQ4MmUtYjA5Yi0xMWZmZWMyMzY2MTggMTcyMDU1Mjc4My45NzgwNDg2",
                    "MD5OfBody": "347ed204873970db803bc71cd15a28f6",
                    "Body": "id_archivo: 10, bucket_name: gmf-bucket, file_name: TGMF-2024062001010001.txt"
                }
            ]
        }
        # Función a testear
        result, error = self.sqs_service.get_messages(
            queue_url, max_messages, wait_time_seconds
        )
        # Validaciones
        self.assertEqual(result,  [
                {
                    "MessageId": "d9c3a65e-eed0-482e-b09b-11ffec236618",
                    "ReceiptHandle": "ZmZiOTUzMjktMzEyMy00NDMzLWI1NjItNjQ3YjhhZmZjZmM5IGFybjphd3M6c3FzOnVzLWVhc3QtMTowMDAwMDAwMDAwMDA6Z21mLXNxcyBkOWMzYTY1ZS1lZWQwLTQ4MmUtYjA5Yi0xMWZmZWMyMzY2MTggMTcyMDU1Mjc4My45NzgwNDg2",
                    "MD5OfBody": "347ed204873970db803bc71cd15a28f6",
                    "Body": "id_archivo: 10, bucket_name: gmf-bucket, file_name: TGMF-2024062001010001.txt"
                }
            ])
        self.assertFalse(error)
        self.sqs_service.logger_service.log_info.assert_called_with(
            "Finaliza obtencion de mensajes del SQS"
        )

    def test_get_messages_failed(self) -> None:
        """Test para excepción en la función get_messages."""
        # Valores mockeados
        queue_url = "test"
        max_messages = 1
        wait_time_seconds = 1
        self.sqs_service.client.receive_message.side_effect = BotoCoreError()
        # Función a testear
        result, error = self.sqs_service.get_messages(
            queue_url, max_messages, wait_time_seconds
        )
        # Validaciones
        self.assertEqual(result, [])
        self.assertTrue(error)
        self.sqs_service.logger_service.log_error.assert_called()

    def test_send_message(self) -> None:
        """Test para la función send_message - success."""
        # Valores mockeados
        queue_url = "test"
        message_body = {"file_id": 123}
        delay_seconds = 10
        self.sqs_service.client.send_message.return_value = None
        # Función a testear
        error = self.sqs_service.send_message(queue_url, message_body, delay_seconds)
        # Validaciones
        self.assertFalse(error)
        self.sqs_service.logger_service.log_info.assert_called_with(
            "Finaliza envio de mensajes al SQS"
        )

    def test_send_message_failed(self) -> None:
        """Test para excepción en la función send_message."""
        # Valores mockeados
        queue_url = "test"
        message_body = {"file_id": 123}
        delay_seconds = 10
        self.sqs_service.client.send_message.side_effect = BotoCoreError()
        # Función a testear
        error = self.sqs_service.send_message(queue_url, message_body, delay_seconds)
        # Validaciones
        self.assertTrue(error)
        self.sqs_service.logger_service.log_error.assert_called()

    def test_delete_message(self) -> None:
        """Test para la función delete_message - success."""
        # Valores mockeados
        queue_url = "test"
        receipt_handle = "receipt_handle"
        self.sqs_service.client.delete_message.return_value = None
        # Función a testear
        error = self.sqs_service.delete_message(queue_url, receipt_handle)
        # Validaciones
        self.assertFalse(error)
        self.sqs_service.logger_service.log_info.assert_called_with(
            "Finaliza eliminacion de mensajes del SQS"
        )

    def test_delete_message_failed(self) -> None:
        """Test para excepción en la función delete_message."""
        # Valores mockeados
        queue_url = "test"
        receipt_handle = "receipt_handle"
        self.sqs_service.client.delete_message.side_effect = BotoCoreError()
        # Función a testear
        error = self.sqs_service.delete_message(queue_url, receipt_handle)
        # Validaciones
        self.assertTrue(error)
        self.sqs_service.logger_service.log_error.assert_called()


if __name__ == '__main__':
    unittest.main()
