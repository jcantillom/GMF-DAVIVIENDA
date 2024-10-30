import unittest
from unittest.mock import patch, MagicMock
from src.core.special_flow import Specialflow
from datetime import datetime 


class TestSpecialflow(unittest.TestCase):

    def setUp(self):
        # Mock services and error handling
        self.mock_services = {
            'env': MagicMock(),  
            'logger_service': MagicMock(),  
            'postgres_service': MagicMock(), 
            'sqs_service': MagicMock(), 
            's3_service': MagicMock(),  
        }
        self.mock_error_handling = MagicMock() 

        # Instantiate the Specialflow class
        self.special_flow_instance = Specialflow(
            services=self.mock_services,
            error_handling=self.mock_error_handling
        )

    @patch('src.core.actions.Actions')  
    def test_special_flow_standard_file(self, mock_actions):
        # Simulate the file being standard (without RE_ESP prefix)
        mock_action_instance = MagicMock()
        mock_actions.return_value = mock_action_instance

        # Simulate a valid file in the database
        self.special_flow_instance.validate_file_in_database = MagicMock(return_value=True)

        # Simulate processing a standard file
        parameters = {
            'file_name': 'standard_file.txt',
            'parameterstore': MagicMock(),
            'error_handling': MagicMock()
        }
        self.special_flow_instance.special_flow(parameters)

        # Assert that process_standard_file was called
        mock_action_instance.normal_flow.assert_called_once()

    @patch('src.core.actions.Actions')  # Mock directo
    def test_special_flow_special_file(self, mock_actions):
        # Simulate the file being special (with RE_ESP prefix)
        mock_action_instance = MagicMock()
        mock_actions.return_value = mock_action_instance

        # Simulate a valid file in the database
        self.special_flow_instance.validate_file_in_database = MagicMock(return_value=True)

        # Simulate processing a special file
        parameters = {
            'file_name': 'RE_ESP_file.txt',
            'parameterstore': {'config-retries': {'start-special-files': 'start', 'end-special-files': 'end'}},
            'error_handling': MagicMock()
        }
        self.special_flow_instance.special_flow(parameters)

        # Assert that process_special_file was called
        self.mock_services['logger_service'].log_info.assert_called_with("Procesando archivo especial")

    def test_validate_file_in_database(self):
        # Simulate a valid file in the database
        self.mock_services['postgres_service'].get_all.return_value = (True, False, '')

        # Execute the method
        result = self.special_flow_instance.validate_file_in_database('test_file.txt')

        # Assert that the file is considered valid
        self.assertTrue(result)

    def test_validate_file_in_database_error(self):
        # Simulate a database error
        self.mock_services['postgres_service'].get_all.return_value = (None, True, 'Database error')

        # Execute the method
        result = self.special_flow_instance.validate_file_in_database('test_file.txt')

        # Assert that the file is considered invalid
        self.assertFalse(result)

    @patch('src.core.actions.Actions')
    @patch('src.core.special_flow.validate_well_formed_esp')
    def test_process_special_file_invalid_format(self, mock_validate_well_formed_esp, mock_actions):
        # Simulate an invalid special file format
        mock_validate_well_formed_esp.return_value = False

        # Call the method
        self.special_flow_instance.process_special_file(
            'RE_ESP_invalid_file.txt', 
            {'config-retries': {'start-special-files': 'start', 'end-special-files': 'end'}}, 
            MagicMock()
        )

        # Assert that the logger logs the error
        self.mock_services['logger_service'].log_error.assert_called_with(
            "El archivo RE_ESP_invalid_file.txt no está bien formado"
        )

    @patch('src.core.actions.Actions')
    @patch('src.core.special_flow.validate_well_formed_esp', return_value=True)
    def test_process_special_file_valid_format(self, mock_validate_well_formed_esp, mock_actions):
        # Mock the Actions class
        mock_action_instance = MagicMock()
        mock_actions.return_value = mock_action_instance

        # Simulate a file that is already in the database
        self.mock_services['postgres_service'].get_all.return_value = (
            [{'id_archivo': '123', 'estado': 'ENVIADO', 'acg_nombre_archivo': 'file'}],
            False, ''
        )

        # Call the method
        self.special_flow_instance.process_special_file(
            'RE_ESP_valid_file.txt',
            {'config-retries': {'start-special-files': 'start', 'end-special-files': 'end'}},
            MagicMock()
        )

        # Assert that the normal_flow method is called
        mock_action_instance.normal_flow.assert_called_once()


    @patch('src.core.actions.Actions')
    def test_process_standard_file(self, mock_actions):
        # Mock the Actions class
        mock_action_instance = MagicMock()
        mock_actions.return_value = mock_action_instance

        # Simulate a file that is already in the database
        self.mock_services['postgres_service'].get_all.return_value = (
            [{'id_archivo': '123', 'estado': 'ENVIADO', 'acg_nombre_archivo': 'file'}],
            False, ''
        )

        # Call the method
        self.special_flow_instance.process_standard_file('standard_file.txt', mock_action_instance)

        # Assert that the normal_flow method is called
        mock_action_instance.normal_flow.assert_called_once()



    def test_insert_new_file_record(self):
        # Mocking postgres insert return value
        self.mock_services['postgres_service'].insert.return_value = (None, False, '')

        # Simulate calling the method with a valid file name
        valid_file_name = 'RE_ESP_VALID_FILE_NAM20240921230444.txt'

        # Overriding format_id_archivo to return a valid integer-convertible ID
        with patch('src.core.format_name_file.format_id_archivo', return_value='12345678'):
            # Call the method insert_new_file_record
            self.special_flow_instance.insert_new_file_record(valid_file_name, MagicMock())

        # Verify that the database insert was called
        self.mock_services['postgres_service'].insert.assert_called_once()


if __name__ == '__main__':
    unittest.main()
