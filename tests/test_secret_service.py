import json
from unittest import TestCase, main
from unittest.mock import patch, MagicMock

# Service
from src.services.secrets_service import SecretsService


class TestSecretService(TestCase):
    """Clase para el manejo de tests de SecretService"""

    @patch("boto3.client")
    def setUp(self, mock_boto_client):
        self.mock_client = MagicMock()
        mock_boto_client.return_value = self.mock_client

        # Mock de valores secretos de AWS Secrets Manager
        self.mock_client.get_secret_value.return_value = {
            "SecretString": '{"POSTGRES_USER": "POSTGRES_USER","POSTGRES_PASSWORD":"POSTGRES_PASSWORD"}'
        }

        # Mock de entorno y logger
        self.mock_env = MagicMock()
        self.mock_env.SECRET_NAME = "my_secret"
        self.mock_env.SECRET_KEY_DATABASE_USER = "POSTGRES_USER"
        self.mock_env.SECRET_KEY_DATABASE_PASSWORD = "POSTGRES_PASSWORD"

        self.mock_logger_service = MagicMock()

        # Claves de secretos a buscar
        self.keys_secrets = [
            self.mock_env.SECRET_KEY_DATABASE_USER,
            self.mock_env.SECRET_KEY_DATABASE_PASSWORD,
        ]

        # Instanciar el servicio de secretos
        self.secret_service = SecretsService(
            env=self.mock_env,
            logger_service=self.mock_logger_service,
            secret_name=self.mock_env.SECRET_NAME,
            keys_secrets=self.keys_secrets,
        )

    def test_secret_service(self) -> None:
        """Test de inicialización de SecretService"""
        # Mock respuesta exitosa de Secrets Manager
        self.mock_client.get_secret_value.return_value = {
            "SecretString": '{"POSTGRES_USER": "POSTGRES_USER"}'
        }

        # Validar que el secret POSTGRES_USER se asignó correctamente
        self.assertEqual(self.secret_service.POSTGRES_USER, "POSTGRES_USER")

        # Validar que los logs fueron llamados correctamente
        self.secret_service.logger_service.log_info.assert_any_call(
            "Inicia proceso para obtener los secrets de AWS Secrets Manager"
        )
        self.secret_service.logger_service.log_info.assert_any_call(
            "Finaliza correctamente el proceso para obtener los secrets de AWS Secrets Manager"
        )

    def test_json_decode_error(self):
        """Test para excepción de JSONDecodeError"""
        # Mock para devolver un JSON inválido
        self.mock_client.get_secret_value.return_value = {
            "SecretString": "invalid_json"
        }

        # Probar que se lanza la excepción correcta
        with self.assertRaises(json.JSONDecodeError):
            self.secret_service._get_secret_value(
                self.mock_env.SECRET_NAME, self.keys_secrets
            )

        # Validar que el logger se llamó en un error fatal
        self.secret_service.logger_service.log_fatal.assert_called_once()

    def test_key_not_in_secret(self):
        """Test para excepción de ValueError cuando falta una clave en el secret"""
        # Mock respuesta sin las claves esperadas
        self.mock_client.get_secret_value.return_value = {
            "SecretString": '{"username": "test_user"}'  # Falta POSTGRES_USER
        }

        # Probar que se lanza un ValueError
        with self.assertRaises(ValueError):
            self.secret_service._get_secret_value(
                self.mock_env.SECRET_NAME, self.keys_secrets
            )

        # Validar que el logger se llamó correctamente
        self.secret_service.logger_service.log_fatal.assert_called()

    def test_successive_initialization(self):
        """Validar comportamiento de Singleton"""
        # Crear una nueva instancia para probar el comportamiento de Singleton
        new_service = SecretsService(
            env=self.mock_env,
            logger_service=self.mock_logger_service,
            secret_name=self.mock_env.SECRET_NAME,
            keys_secrets=self.keys_secrets,
        )
        # Verificar que ambas instancias sean la misma
        self.assertIs(new_service, self.secret_service)

if __name__ == "__main__":
    main()
