import unittest
from unittest.mock import patch, MagicMock
import os
import sys
from main import initialize_services

class TestMain(unittest.TestCase):

    @patch.dict(os.environ, {
        "SERVICE_NAME": "name",
        "REGION_ZONE": "zone",
        "SECRET_NAME": "name",
        "KEYS_SECRETS": '["keys"]',
        "PARAMETER_NAMES": '["parameter"]',
        "DB_HOST": "hots",
        "DB_PORT": "123",
        "DB_NAME": "db",
        "PROCESSED_FILES_FOLDER": "PROCESSED_FILES_FOLDER",
        "FAILED_FILES_FOLDER": "FAILED_FILES_FOLDER",
        "INCONSISTENCIES_FILES_FOLDER": "INCONSISTENCIES_FILES_FOLDER",
        "QUEUE_URL_SOURCE": "http://localhost:4566/queue/test",
        "QUEUE_URL_EMAILS": "http://localhost:4566/queue/test",
        "QUEUE_URL_DESTINATION": "http://localhost:4566/queue/test",
        "DEBUG_MODE": "true",
        "IS_LOCAL": "false",
    })
    @patch('main.LoggerService')
    @patch('main.Environment')
    @patch('main.SecretsService')
    @patch('main.ParameterStoreService')
    @patch('main.S3Service')
    @patch('main.SQSService')
    @patch('main.DatabaseService')
    def test_initialize_services(self, mock_db, mock_sqs, mock_s3, mock_param, mock_secrets, mock_env, mock_logger):
        # Configurar los mocks
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        mock_env_instance = MagicMock()
        mock_env.return_value = mock_env_instance
        
        mock_secrets_instance = MagicMock()
        mock_secrets.return_value = mock_secrets_instance
        
        mock_param_instance = MagicMock()
        mock_param.return_value = mock_param_instance
        
        mock_s3_instance = MagicMock()
        mock_s3.return_value = mock_s3_instance
        
        mock_sqs_instance = MagicMock()
        mock_sqs.return_value = mock_sqs_instance
        
        mock_db_instance = MagicMock()
        mock_db.return_value = mock_db_instance

        # Llamar a la función
        services = initialize_services()

        # Verificar que se crearon todas las instancias de servicios esperadas
        self.assertIsInstance(services['logger_service'], MagicMock)
        self.assertIsInstance(services['env'], MagicMock)
        self.assertIsInstance(services['secrets_service'], MagicMock)
        self.assertIsInstance(services['parameter_store_service'], MagicMock)
        self.assertIsInstance(services['s3_service'], MagicMock)
        self.assertIsInstance(services['sqs_service'], MagicMock)
        self.assertIsInstance(services['postgres_service'], MagicMock)

        # Verificar que se llamaron los constructores con los argumentos correctos
        mock_logger.assert_called_once_with(debug_mode=True)
        mock_env.assert_called_once()
        mock_secrets.assert_called_once()
        mock_param.assert_called_once()
        mock_s3.assert_called_once()
        mock_sqs.assert_called_once()
        mock_db.assert_called_once()

if __name__ == '__main__':
    unittest.main()
