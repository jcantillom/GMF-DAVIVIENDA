import unittest
from unittest.mock import patch, MagicMock, call
from sqlalchemy.exc import SQLAlchemyError
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.services.database_service import DatabaseService
from src.services.secrets_service import SecretsService
from src.utils.environment import Environment

class TestDatabaseService(unittest.TestCase):
    def setUp(self):
        self.mock_env = MagicMock(spec=Environment)
        self.mock_env.DB_HOST = 'localhost'
        self.mock_env.DB_PORT = '5432'
        self.mock_env.DB_NAME = 'test_db'

        self.mock_secrets_service = MagicMock(spec=SecretsService)
        self.mock_secrets_service.USERNAME = 'test_user'
        self.mock_secrets_service.PASSWORD = 'test_password'

        self.mock_logger_service = MagicMock()

        with patch('src.services.database_service.create_engine'):
            self.db_service = DatabaseService(
                env=self.mock_env,
                secrets_service=self.mock_secrets_service,
                logger_service=self.mock_logger_service
            )
        self.db_service.engine = MagicMock()
        self.db_service.session_factory = MagicMock()

    def tearDown(self):
        self.db_service.engine.dispose()

    @patch.object(DatabaseService, '_execute_query')
    def test_get_all_success(self, mock_execute_query):
        mock_model = MagicMock()
        mock_execute_query.return_value = ([{'id': 1, 'name': 'Test'}], False, '')

        result, error, description = self.db_service.get_all(model=mock_model)

        self.assertEqual(result, [{'id': 1, 'name': 'Test'}])
        self.assertFalse(error)
        self.assertEqual(description, '')
        mock_execute_query.assert_called_once()

    @patch.object(DatabaseService, '_execute_query')
    def test_get_by_id_success(self, mock_execute_query):
        mock_model = MagicMock()
        mock_execute_query.return_value = ({'id': 1, 'name': 'Test'}, False, '')

        result, error, description = self.db_service.get_by_id(mock_model, 1)

        self.assertEqual(result, {'id': 1, 'name': 'Test'})
        self.assertFalse(error)
        self.assertEqual(description, '')
        mock_execute_query.assert_called_once()

    @patch.object(DatabaseService, '_execute_query')
    def test_insert_success(self, mock_execute_query):
        mock_model_instance = MagicMock()
        mock_execute_query.return_value = ([], False, '')

        result, error, description = self.db_service.insert(mock_model_instance)

        self.assertEqual(result, [])
        self.assertFalse(error)
        self.assertEqual(description, '')
        mock_execute_query.assert_called_once()

    @patch.object(DatabaseService, '_execute_query')
    def test_insert_many_success(self, mock_execute_query):
        mock_model_instances = [MagicMock(), MagicMock()]
        mock_execute_query.return_value = ([], False, '')

        result, error, description = self.db_service.insert_many(mock_model_instances)

        self.assertEqual(result, [])
        self.assertFalse(error)
        self.assertEqual(description, '')
        mock_execute_query.assert_called_once()

    @patch.object(DatabaseService, '_execute_query')
    def test_update_all_success(self, mock_execute_query):
        mock_model = MagicMock()
        updates = {'name': 'Updated'}
        mock_execute_query.return_value = ([], False, '')

        result, error, description = self.db_service.update_all(mock_model, updates)

        self.assertEqual(result, [])
        self.assertFalse(error)
        self.assertEqual(description, '')
        mock_execute_query.assert_called_once()

    @patch.object(DatabaseService, '_execute_query')
    def test_update_by_id_success(self, mock_execute_query):
        mock_model = MagicMock()
        updates = {'name': 'Updated'}
        mock_execute_query.return_value = ([], False, '')

        result, error, description = self.db_service.update_by_id(mock_model, 1, updates)

        self.assertEqual(result, [])
        self.assertFalse(error)
        self.assertEqual(description, '')
        mock_execute_query.assert_called_once()

    @patch.object(DatabaseService, '_execute_query')
    def test_delete_all_success(self, mock_execute_query):
        mock_model = MagicMock()
        mock_execute_query.return_value = ([], False, '')

        result, error, description = self.db_service.delete_all(mock_model)

        self.assertEqual(result, [])
        self.assertFalse(error)
        self.assertEqual(description, '')
        mock_execute_query.assert_called_once()

    @patch.object(DatabaseService, '_execute_query')
    def test_delete_by_id_success(self, mock_execute_query):
        mock_model = MagicMock()
        mock_execute_query.return_value = ([], False, '')

        result, error, description = self.db_service.delete_by_id(mock_model, 1)

        self.assertEqual(result, [])
        self.assertFalse(error)
        self.assertEqual(description, '')
        mock_execute_query.assert_called_once()

    def test_validate_model_and_columns_success(self):
        mock_model = MagicMock()
        error = self.db_service._validate_model_and_columns(model=mock_model, columns=None)
        self.assertFalse(error)

    def test_validate_model_and_columns_failure(self):
        error = self.db_service._validate_model_and_columns(model=None, columns=None)
        self.assertTrue(error)
        self.db_service.logger_service.log_error.assert_called_once()


    def test_find_record_by_id_success(self):
        mock_session = MagicMock()
        mock_model = MagicMock()
        mock_record = MagicMock()
        mock_query = MagicMock()
        mock_filtered = MagicMock()
        
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_filtered
        mock_filtered.first.return_value = mock_record

        # Configurar el atributo id en el modelo mock
        mock_model.id = MagicMock()

        DatabaseService._find_record_by_id(mock_session, mock_model, 1, {'name': 'Updated'}, 'id')

        # Verificar las llamadas
        mock_session.query.assert_called_once_with(mock_model)
        mock_query.filter.assert_called_once()
        mock_filtered.first.assert_called_once()
        
        # Verificar que se actualizó el registro
        self.assertEqual(mock_record.name, 'Updated')

    def test_find_record_by_id_not_found(self):
        mock_session = MagicMock()
        mock_model = MagicMock()
        mock_query = MagicMock()
        mock_filtered = MagicMock()
        
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_filtered
        mock_filtered.first.return_value = None

        # Configurar el atributo id en el modelo mock
        mock_model.id = MagicMock()

        with self.assertRaises(ValueError):
            DatabaseService._find_record_by_id(mock_session, mock_model, 1, {'name': 'Updated'}, 'id')

        # Verificar las llamadas
        mock_session.query.assert_called_once_with(mock_model)
        mock_query.filter.assert_called_once()
        mock_filtered.first.assert_called_once()


    def test_convert_to_json(self):
        class MockResult:
            def __init__(self, id, name):
                self.id = id
                self.name = name
                self._sa_instance_state = None

        mock_results = [MockResult(1, 'Test')]
        result = DatabaseService._convert_to_json(mock_results)

        self.assertEqual(result, [{'id': 1, 'name': 'Test'}])

    def test_get_by_id_query(self):
        query, params = self.db_service.get_by_id_query('Test', 'DOM')
        
        expected_query = """ 
            select * from cgd_dominios where codigo_dominio = :codigo_dominio and valor = :valor
        """
        expected_params = {
            'valor': 'Test',
            'codigo_dominio': 'DOM'
        }
        
        self.assertEqual(query.strip(), expected_query.strip())
        self.assertEqual(params, expected_params)

    @patch.object(DatabaseService, 'query')
    def test_execute_get_by_id_query(self, mock_query):
        mock_query.return_value = ([{'id': 1, 'valor': 'Test', 'codigo_dominio': 'DOM'}], False, '')
        
        query, params = self.db_service.get_by_id_query('Test', 'DOM')
        result, error, description = self.db_service.query(query, params)
        
        self.assertEqual(result, [{'id': 1, 'valor': 'Test', 'codigo_dominio': 'DOM'}])
        self.assertFalse(error)
        self.assertEqual(description, '')
        mock_query.assert_called_once_with(query, params)

    @patch.object(DatabaseService, '_execute_query')
    def test_get_by_id_all_success(self, mock_execute_query):
        mock_model = MagicMock()
        mock_execute_query.return_value = ([{'id': 1, 'name': 'Test1'}, {'id': 1, 'name': 'Test2'}], False, '')

        result, error, description = self.db_service.get_by_id_all(mock_model, 1)

        self.assertEqual(result, [{'id': 1, 'name': 'Test1'}, {'id': 1, 'name': 'Test2'}])
        self.assertFalse(error)
        self.assertEqual(description, '')
        mock_execute_query.assert_called_once()

    @patch.object(DatabaseService, '_execute_query')
    def test_get_by_id_all_not_found(self, mock_execute_query):
        mock_model = MagicMock()
        mock_execute_query.return_value = ([], False, '')

        result, error, description = self.db_service.get_by_id_all(mock_model, 999)

        self.assertEqual(result, [])
        self.assertFalse(error)
        self.assertEqual(description, '')
        mock_execute_query.assert_called_once()


    @patch.object(DatabaseService, '_execute_query')
    def test_get_by_id_all_with_custom_id(self, mock_execute_query):
        mock_model = MagicMock()
        mock_execute_query.return_value = ([{'custom_id': 1, 'name': 'Test'}], False, '')

        result, error, description = self.db_service.get_by_id_all(mock_model, 1, id_name='custom_id')

        self.assertEqual(result, [{'custom_id': 1, 'name': 'Test'}])
        self.assertFalse(error)
        self.assertEqual(description, '')
        mock_execute_query.assert_called_once()

    @patch.object(DatabaseService, '_execute_query')
    def test_get_by_id_all_with_columns_and_order(self, mock_execute_query):
        mock_model = MagicMock()
        columns = [mock_model.id, mock_model.name]
        order_by = [mock_model.name.asc()]
        mock_execute_query.return_value = ([{'id': 1, 'name': 'Test'}], False, '')

        result, error, description = self.db_service.get_by_id_all(
            mock_model, 1, columns=columns, order_by=order_by
        )

        self.assertEqual(result, [{'id': 1, 'name': 'Test'}])
        self.assertFalse(error)
        self.assertEqual(description, '')
        mock_execute_query.assert_called_once()

    def test_convert_to_json_with_columns(self):
        mock_result = [(1, 'Test')]
        columns = ['id', 'name']

        result = DatabaseService._convert_to_json(mock_result, columns)

        self.assertEqual(result, [{'id': 1, 'name': 'Test'}])


if __name__ == '__main__':
    unittest.main()
