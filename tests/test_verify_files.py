import unittest
from unittest.mock import patch, MagicMock
from src.core.verify_files import Verifyfiles


class TestVerifyFiles(unittest.TestCase):

    def setUp(self):
        # Mockear servicios y objetos necesarios
        self.mock_services = {
            'env': MagicMock(), 
            'logger_service': MagicMock(), 
        }
        self.mock_error_handling = MagicMock()

        # Crear instancia de Verifyfiles usando los mocks
        self.verify_files = Verifyfiles(
            services=self.mock_services,
            error_handling=self.mock_error_handling
        )

    @patch('boto3.client')
    def test_verify_files_data_case_5_files(self, mock_boto_client):
        # Caso con 5 archivos
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_response = {
            'Contents': [
                {'Key': 'path/folder_name/RE_file1.txt'},
                {'Key': 'path/folder_name/RE_file2.txt'},
                {'Key': 'path/folder_name/RE_file3.txt'},
                {'Key': 'path/folder_name/RE_file4.txt'},
                {'Key': 'path/folder_name/RE_file5.txt'}
            ]
        }
        mock_s3.list_objects_v2.return_value = mock_response
        self.mock_services['env'].CONSTANTE_TU_DEBITO_REVERSO = ['file1', 'file2', 'file3']

        # Ejecutar el método
        result = self.verify_files.verify_files_data('test-bucket', 'folder_name', 'path/')
        
        # Validar resultados
        self.assertTrue(result[0])  
        self.assertEqual(result[4], '01')  

    @patch('boto3.client')
    def test_verify_files_data_case_3_files(self, mock_boto_client):
        # Caso con 3 archivos
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_response = {
            'Contents': [
                {'Key': 'path/folder_name/RE_reintegro1.txt'},
                {'Key': 'path/folder_name/RE_reintegro2.txt'},
                {'Key': 'path/folder_name/RE_reintegro3.txt'}
            ]
        }
        mock_s3.list_objects_v2.return_value = mock_response
        self.mock_services['env'].CONSTANTES_TU_REINTEGROS = ['reintegro1', 'reintegro2']

        # Ejecutar el método
        result = self.verify_files.verify_files_data('test-bucket', 'folder_name', 'path/')
        
        # Validar resultados
        self.assertTrue(result[0])  
        self.assertEqual(result[4], '02')  

    @patch('boto3.client')
    def test_verify_files_data_case_2_files(self, mock_boto_client):
        # Caso con 2 archivos
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_response = {
            'Contents': [
                {'Key': 'path/folder_name/RE_especial1.txt'},
                {'Key': 'path/folder_name/RE_especial2.txt'}
            ]
        }
        mock_s3.list_objects_v2.return_value = mock_response
        self.mock_services['env'].CONSTANTES_TU_ESPECIALES = ['especial1', 'especial2']

        # Ejecutar el método
        result = self.verify_files.verify_files_data('test-bucket', 'folder_name', 'path/')
        
        # Validar resultados
        self.assertTrue(result[0]) 
        self.assertEqual(result[4], '03') 

    @patch('boto3.client')
    def test_verify_files_data_no_coincidences(self, mock_boto_client):
        # Caso donde no se encuentran coincidencias
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_response = {'Contents': []} 
        mock_s3.list_objects_v2.return_value = mock_response

        # Ejecutar el método
        result = self.verify_files.verify_files_data('test-bucket', 'folder_name', 'path/')
        
        # Validar resultados
        self.assertFalse(result[0]) 
        self.assertEqual(result[4], '00') 

    @patch('boto3.client')
    def test_verify_files_data_exception(self, mock_boto_client):
        # Simular una excepción
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.list_objects_v2.side_effect = ValueError('Simulated error')

        # Ejecutar el método
        result = self.verify_files.verify_files_data('test-bucket', 'folder_name', 'path/')
        
        # Validar resultados
        self.assertFalse(result[0])  
        self.assertEqual(result[4], '00') 

    def test_validate_file_format(self):
        # Probar un archivo válido
        result = self.verify_files.validate_file_format("test_file.zip")
        self.assertTrue(result)

        # Probar un archivo inválido
        result = self.verify_files.validate_file_format("test_file.txt")
        self.assertFalse(result)
        self.mock_services['logger_service'].log_error.assert_called_with("Formato de archivo inválido para test_file.txt")


if __name__ == '__main__':
    unittest.main()
