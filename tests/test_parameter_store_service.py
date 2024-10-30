import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
import json
from src.services.parameter_store_service import ParameterStoreService
from src.services.logger_service import LoggerService

class TestParameterStoreService(unittest.TestCase):

    def setUp(self):
        self.mock_env = MagicMock()
        self.mock_env.IS_LOCAL = True
        self.mock_env.AWS_REGION = 'us-east-1'
        self.mock_env.LOCALSTACK_ENDPOINT = 'http://localhost:4566'
        self.mock_env.PARAMETER_CONFIG_RETRIES = "/gmf/transversal/config-retries"

        self.mock_logger_service = MagicMock(spec=LoggerService)

        self.parameter_names = [
            self.mock_env.PARAMETER_CONFIG_RETRIES,
        ]
    
        self.patcher = patch("boto3.client")
        self.mock_boto_client = self.patcher.start()
        self.mock_boto_client_instance = self.mock_boto_client.return_value

        # Asegúrate de que el valor del parámetro sea un JSON válido
        self.mock_boto_client_instance.get_parameter.return_value = {
            "Parameter": {
                "Name": "/gmf/transversal/config-retries",
                "Value": '{"number-retries": 3, "time-between-retry": 5}',
            }
        }

        self.parameter_store_service = ParameterStoreService(
            env=self.mock_env,
            logger_service=self.mock_logger_service,
            parameter_names=self.parameter_names,
        )

        # Resetear todos los mocks después de la inicialización
        self.mock_logger_service.reset_mock()
        self.mock_boto_client_instance.reset_mock()

    def tearDown(self):
        self.patcher.stop()

    def test_get_parameters(self):
        """Test success - get_parameters"""
        self.parameter_store_service.get_parameters(self.parameter_names)

        expected_parameters = {
            "transversal": {
                "config-retries": {"number-retries": 3, "time-between-retry": 5}
            }
        }

        self.assertEqual(
            self.parameter_store_service.parameters["transversal"]["config-retries"],
            expected_parameters["transversal"]["config-retries"]
        )

    @patch.object(ParameterStoreService, "_assign_parameter")
    def test_get_parameters_failed(self, mock_assign_parameter):
        """Test exception - get_parameters"""
        mock_assign_parameter.side_effect = json.JSONDecodeError(
            doc="test", msg="error", pos=1
        )
        with self.assertRaises(json.JSONDecodeError):
            self.parameter_store_service.get_parameters(parameter_names=self.parameter_names)

        self.parameter_store_service.logger_service.log_fatal.assert_called_once()

if __name__ == '__main__':
    unittest.main()
