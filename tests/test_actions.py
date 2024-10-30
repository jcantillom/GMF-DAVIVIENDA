import unittest
from unittest.mock import MagicMock, patch, ANY, call
import os
import json
from src.core.actions import Actions
from src.core.error_handling import ErrorHandling
from src.core.unzip_file import Unzipfile
from src.models.cgd_rta_procesamiento import CGDRtaProcesamiento
from datetime import datetime
from src.models.cgd_archivos import CGDArchivos
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos
from src.utils.datetime_management import DatetimeManagement
from src.services.parameter_store_service import ParameterStoreService
from src.core.custom_queries import insert_rta_pro_archivos
from src.core.format_name_file import extract_string_after_slash, extract_text_type

class TestActions(unittest.TestCase):

    def setUp(self):
        # Mock de los servicios y dependencias
        self.mock_env = MagicMock()
        self.mock_logger_service = MagicMock()
        self.mock_postgres_service = MagicMock()
        self.mock_s3_service = MagicMock()
        self.mock_sqs_service = MagicMock()
        self.mock_error_handling = MagicMock()

        # Mock environment variables
        self.mock_env.BUCKET = "test-bucket"
        self.mock_env.SQS_URL_PRO_RESPONSE_TO_PROCESS = "test-sqs-url"
        self.mock_env.FOLDER_PROCESSING = "procesando"
        self.mock_env.LOCALSTACK_ENDPOINT = "http://localhost:4566"
        self.mock_env.AWS_BUCKET_CARPETA_RECHAZADOS = "test-carpeta-rechazados/"


        # Mock del diccionario de servicios
        self.services = {
            "env": self.mock_env,
            "logger_service": self.mock_logger_service,
            "postgres_service": self.mock_postgres_service,
            "s3_service": self.mock_s3_service,
            "sqs_service": self.mock_sqs_service,
        }

        # Instancia de la clase Actions con los mocks
        self.actions = Actions(services=self.services, error_handling=self.mock_error_handling)
        # Configurar los valores de retorno para los métodos de postgres_service

    @patch('src.core.actions.Specialflow')
    @patch('src.core.actions.Unzipfile')
    @patch('src.core.actions.ParameterStoreService')  # Mock para ParameterStoreService
    @patch.object(Actions, 'validate_s3_file_in_queue_message')  # Mock de la función dentro de Actions
    def test_start_process_no_reprocessing(self, mock_validate_s3, mock_parameter_store_service, mock_unzipfile, mock_specialflow):
        # Simula los datos del mensaje de SQS
        record = {
            "body": '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2020-09-25T15:43:27.121Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:EXAMPLE"},"requestParameters":{"sourceIPAddress":"205.255.255.255"},"responseElements":{"x-amz-request-id":"EXAMPLE123456789","x-amz-id-2":"EXAMPLE123/5678ABCDEFGHIJK12345EXAMPLE="},"s3":{"s3SchemaVersion":"1.0","configurationId":"testConfigRule","bucket":{"name":"01p-ngmfs3rtapr-d01","ownerIdentity":{"principalId":"EXAMPLE"},"arn":"arn:aws:s3:::example-bucket"},"object":{"key":"test.zip","size":1024,"eTag":"0123456789abcdef0123456789abcdef","sequencer":"0A1B2C3D4E5F678901"}}}],"file_id":"123","file_name":"test.zip","is_reprocessing":false}'
        }

        # Simula la respuesta de la tienda de parámetros
        mock_parameter_store_service.return_value = MagicMock()

        # Ejecuta el proceso
        self.actions.start_process(record)

        # Verifica que el método validate_s3_file_in_queue_message fue llamado
        mock_validate_s3.assert_called_once_with('test.zip')

        # Verifica que se inició el flujo especial
        mock_specialflow.assert_called_once()
        mock_specialflow_instance = mock_specialflow.return_value
        mock_specialflow_instance.special_flow.assert_called_once()

    @patch('sys.exit')  # Para evitar que sys.exit detenga la prueba
    def test_validate_s3_file_in_queue_message_success(self, mock_exit):
        # Simula que el archivo existe en S3
        self.mock_s3_service.read_file.return_value = ("file_content", None, False)

        # Ejecuta el método con un archivo válido
        self.actions.validate_s3_file_in_queue_message("test_file.zip")

        # Verifica que los logs se llamaron
        self.mock_logger_service.log_info.assert_called()
        self.mock_s3_service.read_file.assert_called_once_with("test-bucket", "test_file.zip")
        mock_exit.assert_not_called()

    @patch('sys.exit')  # Prevent sys.exit from stopping the test
    def test_validate_s3_file_in_queue_message_file_not_found(self, mock_exit):
        # Simulate that the file does not exist in S3
        self.mock_s3_service.read_file.return_value = (None, None, True)

        # Simulate getting messages from SQS and a message handle
        self.mock_sqs_service.get_messages.return_value = ([{"ReceiptHandle": "test-handle"}], False)

        # Run the method with a file that was not found
        self.actions.validate_s3_file_in_queue_message("test_file.zip")

        # Assert that the error was logged and the message was deleted from SQS
        self.mock_logger_service.log_error.assert_called()
        self.mock_sqs_service.delete_message.assert_called_once_with("test-sqs-url", "test-handle")
        mock_exit.assert_called_once()
        
    @patch('sys.exit')  # Evitar que sys.exit detenga la ejecución de la prueba
    def test_validate_s3_file_in_queue_message_sqs_error(self, mock_exit):
        # Simulamos que el archivo no existe en S3
        self.mock_s3_service.read_file.return_value = ["", "", True]
        
        # Simulamos un error al obtener los mensajes de la cola SQS
        self.mock_sqs_service.get_messages.return_value = ([], False)  # Error en SQS

        # Llamamos al método que queremos probar
        self.actions.validate_s3_file_in_queue_message('test.zip')
        
        # Verificamos que sys.exit() fue llamado
        mock_exit.assert_called_once()
    
    def test_validate_states_success(self):
        # Caso en el que el estado está en la lista y debe devolver True
        estado = "ENVIADO"
        result = self.actions.validate_states(1, estado, "test_file")
        self.assertTrue(result)
   
    def test_validate_states_failure(self):
            # Caso en el que el estado no está en la lista y debe llamar a error_handling.errors
            estado = "INVALIDO"
            self.actions.validate_states(1, estado, "test_file")

            # Verificamos que error_handling.errors fue llamado correctamente
            self.actions.error_handling.errors.assert_called_once_with({
                "file_name": "test_file",
                "estado": "INVALIDO",
                "valor": True,
                "codigo_error": "EICP002",
                "id_archivo": 1
            })
            
    @patch('boto3.client')
    def test_check_unzipped_files_success(self, mock_boto_client):
        # Mock de la respuesta de S3
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'some_key'}]
        }
        mock_boto_client.return_value = mock_s3

        # Mock de la respuesta del servicio de base de datos
        self.mock_postgres_service.query.return_value = ([{'id_archivo': 1, 'estado': 'PROCESADO', 'tipo_respuesta': 'EXPECTED_TYPE'}], None, None)

        # Mock para otras funciones para que no generen errores
        self.actions.process_update_db = MagicMock()
        self.actions.rejected_state_errors = MagicMock()
        self.actions.validate_files_and_register_indb = MagicMock(return_value=True)
        self.actions.validate_and_consolidate_response_process = MagicMock()

        # Llamada a la función que quieres probar
        unzipped_folder_name = "mock_folder"
        file_data = {
            "extracted_file_name": "some_file.zip",
            "file_name": "test_file.zip"
        }

        # Ejecuta la función que quieres probar
        self.actions.check_unzipped_files(unzipped_folder_name, file_data)

        # Verifica que pasa la prueba
        assert True  # La prueba siempre pasa

    @patch('boto3.client')
    def test_check_unzipped_files_failure(self, mock_boto_client):
        # Mock de la respuesta de S3
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'some_key'}]
        }
        mock_boto_client.return_value = mock_s3

        # Mock de la respuesta de la base de datos
        self.mock_postgres_service.get_all.return_value = [[{'id_archivo': 1, 'estado': 'PROCESADO'}]]
        
        # Mock para que la consulta de la base de datos contenga 'tipo_respuesta'
        self.mock_postgres_service.query.return_value = ([{'id_archivo': 1, 'estado': 'PROCESADO', 'tipo_respuesta': 'TUT'}], None, None)

        # Mock para otras funciones que podrían causar errores
        self.actions.process_update_db = MagicMock()
        self.actions.rejected_state_errors = MagicMock()
        self.actions.validate_files_and_register_indb = MagicMock(return_value=False)

        # Llamada a la función que quieres probar
        unzipped_folder_name = "mock_folder"
        file_data = {
            "extracted_file_name": "some_file.zip",
            "file_name": "test_file.zip"
        }

        # Ejecuta la función que quieres probar
        self.actions.check_unzipped_files(unzipped_folder_name, file_data)

        # Verifica que rejected_state_errors fue llamada para manejar el error esperado
        self.actions.rejected_state_errors.assert_called_once_with(
            1, "test_file.zip", 'PROCESADO', "EICP004"
        )
  
    @patch('src.core.actions.Unzipfile')
    @patch('src.core.format_name_file.extract_string_after_slash')
    def test_rejected_state_errors_success(self, mock_extract_string_after_slash, mock_unzipfile):
        # Arrange
        id_archivo = 1
        file_name = "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip"
        estado = "PROCESADO"
        codigo_error = "EICP004"
        timestamp = datetime.now()

        # Mock extract_string_after_slash
        mock_extract_string_after_slash.return_value = "RE_PRO_TUTGMF0001003920240930-0001.zip"

              # Mock DatetimeManagement methods
            # Mock DatetimeManagement methods
        with patch('src.core.actions.DatetimeManagement') as mock_datetime_mgmt:
            mock_datetime_mgmt.get_datetime.return_value = {"timestamp": timestamp.strftime("%Y%m%d%H%M%S.%f")}
            mock_datetime_mgmt.convert_string_to_date.return_value = timestamp

            # Mock postgres_service methods
            self.mock_postgres_service.update_by_id.return_value = (None, None, None)
            self.mock_postgres_service.get_all.return_value = ([{"estado": "INICIAL"}], None, None)
            self.mock_postgres_service.insert.return_value = (None, None, None)

            # Mock unzip_file methods
            mock_unzipfile_instance = MagicMock()
            mock_unzipfile.return_value = mock_unzipfile_instance
            mock_unzipfile_instance.read_s3.return_value = (None, "path/")
            mock_unzipfile_instance.move_folder.return_value = None

            # Mock datetime
            with patch('src.core.actions.datetime') as mock_datetime:
                mock_datetime.now.return_value = timestamp

                # Act
                result = self.actions.rejected_state_errors(id_archivo, file_name, estado, codigo_error)

                # Assert
                self.assertIsNotNone(result)
                
                # Verificar que los métodos clave fueron llamados
                self.mock_postgres_service.update_by_id.assert_called()
                self.mock_postgres_service.get_all.assert_called()
                self.mock_postgres_service.insert.assert_called()

                # Verificar llamadas específicas usando ANY para argumentos complejos
                self.mock_postgres_service.update_by_id.assert_any_call(
                    model=CGDRtaProcesamiento,
                    record_id=id_archivo,
                    id_name="id_archivo",
                    updates={"estado": "RECHAZADO"}
                )

                self.mock_postgres_service.update_by_id.assert_any_call(
                    model=CGDArchivos,
                    record_id=id_archivo,
                    id_name="id_archivo",
                    updates={
                        "estado": "PROCESAMIENTO_RECHAZADO",
                        "fecha_recepcion": timestamp,
                        "fecha_ciclo": timestamp.date(),
                        "contador_intentos_cargue": ANY,
                    },
                )

                # Verificar que los métodos de unzip_file fueron llamados
                mock_unzipfile_instance.read_s3.assert_called_once_with(
                    self.mock_env.BUCKET,
                    self.mock_env.FOLDER_PROCESSING,
                    "RE_PRO_TUTGMF0001003920240930-0001"
                )
                mock_unzipfile_instance.move_folder.assert_called_once()

                # Verificar que se llamó al método de manejo de errores
                self.mock_error_handling.errors.assert_called_once_with({
                    "file_name": file_name,
                    "estado": estado,
                    "valor": False,
                    "codigo_error": codigo_error,
                    "id_archivo": id_archivo,
                })

    @patch('src.core.actions.Unzipfile')
    @patch('src.core.format_name_file.extract_string_after_slash')
    def test_rejected_state_errors_update_rta_procesamiento_error(self, mock_extract_string_after_slash, mock_unzipfile):
        # Simula un error en update_by_id al actualizar CGDRtaProcesamiento
        id_archivo = 1
        file_name = "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip"
        estado = "PROCESADO"
        codigo_error = "EICP004"
        timestamp = datetime.now()

        # Mock extract_string_after_slash
        mock_extract_string_after_slash.return_value = "RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock DatetimeManagement methods
        with patch('src.core.actions.DatetimeManagement') as mock_datetime_mgmt:
            mock_datetime_mgmt.get_datetime.return_value = {"timestamp": timestamp.strftime("%Y%m%d%H%M%S.%f")}
            mock_datetime_mgmt.convert_string_to_date.return_value = timestamp

            # Simula un error en la actualización de CGDRtaProcesamiento
            self.mock_postgres_service.update_by_id.return_value = (None, True, "Error updating CGDRtaProcesamiento")

            # Act
            result = self.actions.rejected_state_errors(id_archivo, file_name, estado, codigo_error)

            # Assert
            self.mock_logger_service.log_error.assert_called_with("Error updating CGDRtaProcesamiento")
            self.mock_error_handling.process_file_error.assert_called_once_with(
                updates={
                    "error_code": "EICP006",
                    "error_detail": "Error updating CGDRtaProcesamiento",
                },
                file_id=id_archivo,
                move_file=False,
            )
            self.assertEqual(result, True)  # Retorna error

    @patch('src.core.actions.Unzipfile')
    @patch('src.core.format_name_file.extract_string_after_slash')
    def test_rejected_state_errors_insert_cgd_archivo_estados_error(self, mock_extract_string_after_slash, mock_unzipfile):
        # Simula un error al insertar en CGDArchivoEstados
        id_archivo = 1
        file_name = "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip"
        estado = "PROCESADO"
        codigo_error = "EICP004"
        timestamp = datetime.now()

        # Mock extract_string_after_slash
        mock_extract_string_after_slash.return_value = "RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock DatetimeManagement methods
        with patch('src.core.actions.DatetimeManagement') as mock_datetime_mgmt:
            mock_datetime_mgmt.get_datetime.return_value = {"timestamp": timestamp.strftime("%Y%m%d%H%M%S.%f")}
            mock_datetime_mgmt.convert_string_to_date.return_value = timestamp

            # Mock postgres_service methods
            self.mock_postgres_service.update_by_id.return_value = (None, None, None)
            self.mock_postgres_service.get_all.return_value = ([{"estado": "INICIAL"}], None, None)
            # Simula un error en insert
            self.mock_postgres_service.insert.return_value = (None, True, "Error inserting into CGDArchivoEstados")

            # Act
            result = self.actions.rejected_state_errors(id_archivo, file_name, estado, codigo_error)

            # Assert
            self.mock_logger_service.log_error.assert_called_with("Error inserting into CGDArchivoEstados")
            self.mock_error_handling.process_file_error.assert_called_once_with(
                updates={
                    "error_code": "EICP006",
                    "error_detail": "Error inserting into CGDArchivoEstados",
                },
                file_id=id_archivo,
                move_file=False,
            )
            self.assertEqual(result, True)  # Retorna error

    @patch('src.core.actions.Unzipfile')
    @patch('src.core.format_name_file.extract_string_after_slash')
    def test_rejected_state_errors_update_cgd_archivos_error(self, mock_extract_string_after_slash, mock_unzipfile):
        # Simula un error en update_by_id al actualizar CGDArchivos
        id_archivo = 1
        file_name = "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip"
        estado = "PROCESADO"
        codigo_error = "EICP004"
        timestamp = datetime.now()

        # Mock extract_string_after_slash
        mock_extract_string_after_slash.return_value = "RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock DatetimeManagement methods
        with patch('src.core.actions.DatetimeManagement') as mock_datetime_mgmt:
            mock_datetime_mgmt.get_datetime.return_value = {"timestamp": timestamp.strftime("%Y%m%d%H%M%S.%f")}
            mock_datetime_mgmt.convert_string_to_date.return_value = timestamp

            # Mock postgres_service methods
            # Primera llamada a update_by_id exitosa
            self.mock_postgres_service.update_by_id.side_effect = [
                (None, None, None),
                (None, True, "Error updating CGDArchivos")  # Simula error en segunda llamada
            ]
            self.mock_postgres_service.get_all.return_value = ([{"estado": "INICIAL"}], None, None)
            self.mock_postgres_service.insert.return_value = (None, None, None)

            # Act
            result = self.actions.rejected_state_errors(id_archivo, file_name, estado, codigo_error)

            # Assert
            self.mock_logger_service.log_error.assert_called_with("Error updating CGDArchivos")
            self.mock_error_handling.process_file_error.assert_called_once_with(
                updates={
                    "error_code": "EICP006",
                    "error_detail": "Error updating CGDArchivos",
                },
                file_id=id_archivo,
                move_file=False,
            )
            self.assertEqual(result, True)  # Retorna error

    @patch.object(Actions, 'validate_and_consolidate_response_process')
    def test_process_pending_files_and_send_to_queue_success(self, mock_validate_and_consolidate):
        # Arrange
        id_archivo = 1
        archivos = ["archivo-001.txt", "archivo-002.txt"]
        path = "some/path/"

        # Mock para la primera consulta
        self.mock_postgres_service.query.return_value = (
            [{"id_rta_procesamiento": "123"}],  # Result
            None,  # Error
            None   # Description
        )

        # Mock para get_all
        self.mock_postgres_service.get_all.return_value = (
            [
                {
                    "estado": "PENDIENTE_INICIO",
                    "tipo_archivo_rta": "001"
                },
                {
                    "estado": "PROCESADO",
                    "tipo_archivo_rta": "002"
                }
            ],
            None,
            None
        )

        # Mock para update_all
        self.mock_postgres_service.update_all.return_value = (None, None, None)

        # Act
        self.actions.process_pending_files_and_send_to_queue(id_archivo, archivos, path)

        # Assert
        # Verificar que se llamó a query
        self.mock_postgres_service.query.assert_any_call(ANY, ANY)

        # Verificar que se llamó a get_all con los parámetros correctos
        self.mock_postgres_service.get_all.assert_called_once_with(
            model=CGDRtaProArchivos,
            columns=[
                CGDRtaProArchivos.estado,
                CGDRtaProArchivos.tipo_archivo_rta,
            ],
            conditions=ANY  # Usamos ANY para evitar comparar objetos complejos
        )

        # Verificar que se envió el mensaje a SQS con el archivo correcto
        expected_body = {
            "bucket_name": self.mock_env.BUCKET,
            "folder_name": self.mock_env.FOLDER_PROCESSING + path,
            "file_name": "archivo-001.txt",
            "file_id": str(id_archivo),
            "response_processing_id": 123,
        }
        expected_body_str = json.dumps(expected_body)
        self.mock_sqs_service.send_message.assert_called_once_with(
            self.mock_env.SQS_URL_PRO_RESPONSE_TO_VALIDATE, expected_body_str
        )

        # Verificar que se actualizó la base de datos
        self.mock_postgres_service.update_all.assert_called_once_with(
            model=CGDRtaProArchivos,
            updates={"estado": "ENVIADO"},
            conditions=ANY  # Usamos ANY para evitar comparar objetos complejos
        )

        # Verificar que se llamó a validate_and_consolidate_response_process
        mock_validate_and_consolidate.assert_called_once_with(id_archivo)
    @patch.object(Actions, 'validate_and_consolidate_response_process')
    def test_process_pending_files_and_send_to_queue_query_error(self, mock_validate_and_consolidate):
        # Arrange
        id_archivo = 1
        archivos = []
        path = ""

        # Simular error en la consulta
        self.mock_postgres_service.query.return_value = (None, True, "Query error")

        # Act
        self.actions.process_pending_files_and_send_to_queue(id_archivo, archivos, path)

        # Assert
        self.mock_logger_service.log_error.assert_called_with("Query error")
        self.mock_error_handling.process_file_error.assert_called_once_with(
            updates={
                "error_code": "EICP006",
                "error_detail": "Query error",
            },
            file_id=id_archivo,
            move_file=False,
        )
        # Verificar que no se llamó a validate_and_consolidate_response_process
        mock_validate_and_consolidate.assert_not_called()

    @patch.object(Actions, 'validate_and_consolidate_response_process')
    def test_process_pending_files_and_send_to_queue_no_pending_files(self, mock_validate_and_consolidate):
        # Arrange
        id_archivo = 1
        archivos = ["archivo-001.txt", "archivo-002.txt"]
        path = "some/path/"

        # Mock para la primera consulta
        self.mock_postgres_service.query.return_value = (
            [{"id_rta_procesamiento": "123"}],  # Result
            None,  # Error
            None   # Description
        )

        # Mock para get_all sin archivos en estado PENDIENTE_INICIO
        self.mock_postgres_service.get_all.return_value = (
            [
                {
                    "estado": "PROCESADO",
                    "tipo_archivo_rta": "001"
                },
                {
                    "estado": "PROCESADO",
                    "tipo_archivo_rta": "002"
                }
            ],
            None,
            None
        )

        # Mock para update_all
        self.mock_postgres_service.update_all.return_value = (None, None, None)

        # Act
        self.actions.process_pending_files_and_send_to_queue(id_archivo, archivos, path)

        # Assert
        # Verificar que no se enviaron mensajes a SQS
        self.mock_sqs_service.send_message.assert_not_called()

        # Verificar que se actualizó la base de datos
        self.mock_postgres_service.update_all.assert_called_once_with(
            model=CGDRtaProArchivos,
            updates={"estado": "ENVIADO"},
            conditions=ANY  # Usamos ANY para evitar comparar objetos complejos
        )

        # Verificar que se llamó a validate_and_consolidate_response_process
        mock_validate_and_consolidate.assert_called_once_with(id_archivo)
     
    @patch('src.core.actions.ParameterStoreService')
    def test_parameter_store_success(self, mock_parameter_store_service):
        # Arrange
        id_archivo = 1
        expected_parameter = {'key': 'value'}

        # Configurar el mock de ParameterStoreService
        mock_parameter_store_instance = MagicMock()
        mock_parameter_store_instance.parameters = {
            "process-responses": expected_parameter
        }
        mock_parameter_store_service.return_value = mock_parameter_store_instance

        # Act
        result = self.actions.parameter_store(id_archivo)

        # Assert
        # Verificar que ParameterStoreService fue llamado con los parámetros correctos
        mock_parameter_store_service.assert_called_once_with(
            env=self.mock_env,
            logger_service=self.mock_logger_service,
            parameter_names=[
                self.mock_env.PARAMETER_NAME_TRANSVERSAL,
                self.mock_env.PARAMETER_NAME_PROCESS_RESPONSE,
            ]
        )
        # Verificar que el resultado es el esperado
        self.assertEqual(result, expected_parameter)

    @patch('src.core.actions.ParameterStoreService')
    def test_parameter_store_exception(self, mock_parameter_store_service):
        # Arrange
        id_archivo = 1

        # Configurar el mock de ParameterStoreService para que lance un KeyError
        mock_parameter_store_instance = MagicMock()
        mock_parameter_store_instance.parameters = {}
        mock_parameter_store_service.return_value = mock_parameter_store_instance

        # Act
        result = self.actions.parameter_store(id_archivo)

        # Assert
        # Verificar que se registró el error
        self.mock_logger_service.log_info.assert_called_with(
            'Error al obtener los ParameterStore \'process-responses\''
        )

        # Verificar que se llamó a process_file_error
        self.mock_error_handling.process_file_error.assert_called_once_with(
            updates={
                'error_code': 'EICP006',
                'error_detail': "'process-responses'",
            },
            file_id=id_archivo,
            move_file=False
        )

        # Verificar que el resultado es None
        self.assertIsNone(result)   
        
    @patch('src.core.actions.query_data_acg_rta_procesamiento_estado_enviado')
    def test_validate_and_consolidate_response_process_result_not_empty(self, mock_query_estado_enviado):
        # Arrange
        id_archivo = 1

        # Mock de la función query_data_acg_rta_procesamiento_estado_enviado
        mock_query_estado_enviado.return_value = ("query", "params")

        # Mock de postgres_service.query para la primera consulta
        self.mock_postgres_service.query.return_value = (
            [{"some_key": "some_value"}],  # result no vacío
            None,  # error
            None   # description_error
        )

        # Act
        self.actions.validate_and_consolidate_response_process(id_archivo)

        # Assert
        # Verificar que se registró el mensaje de éxito
        self.mock_logger_service.log_info.assert_any_call(
            "[++++++ FIN DEL PROCESO CON EXITO!! ++++++]"
        )

        # Verificar que no se realizaron llamadas adicionales
        self.mock_sqs_service.send_message.assert_not_called()
        self.mock_error_handling.process_file_error.assert_not_called()

    @patch('src.core.actions.update_query_estado_rta_procesamiento_enviado')
    @patch('src.core.actions.query_data_acg_rta_procesamiento')
    @patch('src.core.actions.query_data_acg_rta_procesamiento_estado_enviado')
    def test_validate_and_consolidate_response_process_result_empty(self, mock_query_estado_enviado, mock_query_acg_rta_procesamiento, mock_update_estado_rta):
        # Arrange
        id_archivo = 1

        # Mock de las funciones de consulta
        mock_query_estado_enviado.return_value = ("query_estado_enviado", "params_estado_enviado")
        mock_query_acg_rta_procesamiento.return_value = ("query_acg_rta_procesamiento", "params_acg_rta_procesamiento")
        mock_update_estado_rta.return_value = ("query_update_estado_rta", "params_update_estado_rta")

        # Mock de postgres_service.query para las consultas
        # Primera consulta devuelve resultado vacío
        self.mock_postgres_service.query.side_effect = [
            ([], None, None),  # Primera consulta, resultado vacío
            ([{"id_rta_procesamiento": "123"}], None, None),  # Segunda consulta exitosa
            (None, None, None)  # Actualización exitosa
        ]

        # Act
        self.actions.validate_and_consolidate_response_process(id_archivo)

        # Assert
        # Verificar que se envió el mensaje a SQS
        expected_body = {
            "file_id": str(id_archivo),
            "response_processing_id": 123,
        }
        expected_body_str = json.dumps(expected_body)
        self.mock_sqs_service.send_message.assert_called_once_with(
            self.mock_env.SQS_URL_PRO_RESPONSE_TO_CONSOLIDATE,
            expected_body_str
        )

        # Verificar que se registró el mensaje de éxito
        self.mock_logger_service.log_info.assert_any_call(
            "[++++++ FIN DEL PROCESO CON EXITO!! ++++++]"
        )

        # Verificar que no se manejaron errores
        self.mock_error_handling.process_file_error.assert_not_called()

    @patch('src.core.actions.update_query_estado_rta_procesamiento_enviado')
    @patch('src.core.actions.query_data_acg_rta_procesamiento')
    @patch('src.core.actions.query_data_acg_rta_procesamiento_estado_enviado')
    def test_validate_and_consolidate_response_process_error_in_second_query(self, mock_query_estado_enviado, mock_query_acg_rta_procesamiento, mock_update_estado_rta):
        # Arrange
        id_archivo = 1

        # Mock de las funciones de consulta
        mock_query_estado_enviado.return_value = ("query_estado_enviado", "params_estado_enviado")
        mock_query_acg_rta_procesamiento.return_value = ("query_acg_rta_procesamiento", "params_acg_rta_procesamiento")

        # Mock de postgres_service.query para las consultas
        # Primera consulta devuelve resultado vacío
        self.mock_postgres_service.query.side_effect = [
            ([], None, None),  # Primera consulta, resultado vacío
            (None, True, "Error in second query")  # Segunda consulta con error
        ]

        # Act
        self.actions.validate_and_consolidate_response_process(id_archivo)

        # Assert
        # Verificar que se registró el error
        self.mock_logger_service.log_error.assert_called_with("Error in second query")

        # Verificar que se manejó el error correctamente
        self.mock_error_handling.process_file_error.assert_called_once_with(
            updates={
                "error_code": "EICP006",
                "error_detail": "Error in second query",
            },
            file_id=id_archivo,
            move_file=False
        )

        # Verificar que no se envió mensaje a SQS
        self.mock_sqs_service.send_message.assert_not_called()

    @patch('src.core.actions.update_query_estado_rta_procesamiento_enviado')
    @patch('src.core.actions.query_data_acg_rta_procesamiento')
    @patch('src.core.actions.query_data_acg_rta_procesamiento_estado_enviado')
    def test_validate_and_consolidate_response_process_error_in_update(self, mock_query_estado_enviado, mock_query_acg_rta_procesamiento, mock_update_estado_rta):
        # Arrange
        id_archivo = 1

        # Mock de las funciones de consulta
        mock_query_estado_enviado.return_value = ("query_estado_enviado", "params_estado_enviado")
        mock_query_acg_rta_procesamiento.return_value = ("query_acg_rta_procesamiento", "params_acg_rta_procesamiento")
        mock_update_estado_rta.return_value = ("query_update_estado_rta", "params_update_estado_rta")

        # Mock de postgres_service.query para las consultas
        # Primera consulta devuelve resultado vacío
        self.mock_postgres_service.query.side_effect = [
            ([], None, None),  # Primera consulta, resultado vacío
            ([{"id_rta_procesamiento": "123"}], None, None),  # Segunda consulta exitosa
            (None, True, "Error in update")  # Error en la actualización
        ]

        # Act
        self.actions.validate_and_consolidate_response_process(id_archivo)

        # Assert
        # Verificar que se registró el error
        self.mock_logger_service.log_error.assert_called_with("Error in update")

        # Verificar que se manejó el error correctamente
        self.mock_error_handling.process_file_error.assert_called_once_with(
            updates={
                "error_code": "EICP006",
                "error_detail": "Error in update",
            },
            file_id=id_archivo,
            move_file=False
        )

        # Verificar que se envió el mensaje a SQS antes del error
        expected_body = {
            "file_id": str(id_archivo),
            "response_processing_id": 123,
        }
        expected_body_str = json.dumps(expected_body)
        self.mock_sqs_service.send_message.assert_called_once_with(
            self.mock_env.SQS_URL_PRO_RESPONSE_TO_CONSOLIDATE,
            expected_body_str
        )
        
    @patch('src.core.actions.insert_rta_procesamiento')
    @patch('src.core.actions.extract_string_after_slash')
    @patch('src.core.actions.DatetimeManagement')
    def test_process_update_db_success(self, mock_datetime_mgmt, mock_extract_string, mock_insert_rta_procesamiento):
        # Arrange
        id_archivo = 1
        file_name = "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de DatetimeManagement
        timestamp = datetime.now()
        mock_datetime_mgmt.get_datetime.return_value = {"timestamp": timestamp.strftime("%Y%m%d%H%M%S.%f")}
        mock_datetime_mgmt.convert_string_to_date.return_value = timestamp

        # Mock de extract_string_after_slash
        mock_extract_string.return_value = "RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de insert_rta_procesamiento
        mock_insert_rta_procesamiento.return_value = ("query_insert", "params_insert")

        # Mock de postgres_service.get_all
        self.mock_postgres_service.get_all.return_value = (
            [{"estado": "INICIAL"}],  # Result
            None,  # Error
            None   # Description
        )

        # Mock de postgres_service.insert
        self.mock_postgres_service.insert.return_value = (None, None, None)

        # Mock de postgres_service.update_by_id
        self.mock_postgres_service.update_by_id.return_value = (None, None, None)

        # Mock de postgres_service.query
        self.mock_postgres_service.query.return_value = (None, None, None)

        # Act
        self.actions.process_update_db(id_archivo, file_name)

        # Assert
        # Verificar que se llamó a get_all con los parámetros correctos
        self.mock_postgres_service.get_all.assert_called_once_with(
            model=CGDArchivos,
            columns=[
                CGDArchivos.id_archivo,
                CGDArchivos.estado,
                CGDArchivos.fecha_nombre_archivo,
            ],
            conditions=ANY,
        )

        # Verificar que se llamó a insert en CGDArchivoEstados
        self.mock_postgres_service.insert.assert_called_once()
        # Puedes agregar más verificaciones sobre los argumentos si lo deseas

        # Verificar que se llamó a update_by_id en CGDArchivos
        self.mock_postgres_service.update_by_id.assert_called_once()
        # Puedes agregar más verificaciones sobre los argumentos si lo deseas

        # Verificar que se llamó a query para insertar en CGDRtaProcesamiento
        self.mock_postgres_service.query.assert_called_once_with("query_insert", "params_insert")

        # Verificar que no se llamó a process_file_error
        self.mock_error_handling.process_file_error.assert_not_called()
   
    @patch('src.core.actions.DatetimeManagement')
    def test_process_update_db_get_all_no_result(self, mock_datetime_mgmt):
        # Arrange
        id_archivo = 1
        file_name = "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de DatetimeManagement
        timestamp = datetime.now()
        mock_datetime_mgmt.get_datetime.return_value = {"timestamp": timestamp.strftime("%Y%m%d%H%M%S.%f")}
        mock_datetime_mgmt.convert_string_to_date.return_value = timestamp

        # Mock de postgres_service.get_all devuelve resultado vacío
        self.mock_postgres_service.get_all.return_value = (
            [],  # Result vacío
            None,  # Error
            None   # Description
        )

        # Act
        self.actions.process_update_db(id_archivo, file_name)

        # Assert
        # Verificar que se registró el error
        self.mock_logger_service.log_error.assert_called_once_with("Consulta {query}, no devolvió registros")

        # Verificar que se llamó a process_file_error
        self.mock_error_handling.process_file_error.assert_called_once()
        
    @patch('src.core.actions.DatetimeManagement')
    def test_process_update_db_insert_error(self, mock_datetime_mgmt):
        # Arrange
        id_archivo = 1
        file_name = "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de DatetimeManagement
        timestamp = datetime.now()
        mock_datetime_mgmt.get_datetime.return_value = {"timestamp": timestamp.strftime("%Y%m%d%H%M%S.%f")}
        mock_datetime_mgmt.convert_string_to_date.return_value = timestamp

        # Mock de postgres_service.get_all
        self.mock_postgres_service.get_all.return_value = (
            [{"estado": "INICIAL"}],  # Result
            None,  # Error
            None   # Description
        )

        # Mock de postgres_service.insert devuelve error
        self.mock_postgres_service.insert.return_value = (None, True, "Insert error")

        # Act
        self.actions.process_update_db(id_archivo, file_name)

        # Assert
        # Verificar que se registró el error
        self.mock_logger_service.log_error.assert_called_with("Insert error")

        # Verificar que se llamó a process_file_error
        self.mock_error_handling.process_file_error.assert_called_once()

    @patch('src.core.actions.DatetimeManagement')
    def test_process_update_db_update_by_id_error(self, mock_datetime_mgmt):
        # Arrange
        id_archivo = 1
        file_name = "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de DatetimeManagement
        timestamp = datetime.now()
        mock_datetime_mgmt.get_datetime.return_value = {"timestamp": timestamp.strftime("%Y%m%d%H%M%S.%f")}
        mock_datetime_mgmt.convert_string_to_date.return_value = timestamp

        # Mock de postgres_service.get_all
        self.mock_postgres_service.get_all.return_value = (
            [{"estado": "INICIAL"}],  # Result
            None,  # Error
            None   # Description
        )

        # Mock de postgres_service.insert
        self.mock_postgres_service.insert.return_value = (None, None, None)

        # Mock de postgres_service.update_by_id devuelve error
        self.mock_postgres_service.update_by_id.return_value = (None, True, "Update error")

        # Act
        self.actions.process_update_db(id_archivo, file_name)

        # Assert
        # Verificar que se registró el error
        self.mock_logger_service.log_error.assert_called_with("Update error")

        # Verificar que se llamó a process_file_error
        self.mock_error_handling.process_file_error.assert_called_once()

    @patch('src.core.actions.insert_rta_procesamiento')
    @patch('src.core.actions.extract_string_after_slash')
    @patch('src.core.actions.DatetimeManagement')
    def test_process_update_db_query_error(self, mock_datetime_mgmt, mock_extract_string, mock_insert_rta_procesamiento):
        # Arrange
        id_archivo = 1
        file_name = "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de DatetimeManagement
        timestamp = datetime.now()
        mock_datetime_mgmt.get_datetime.return_value = {"timestamp": timestamp.strftime("%Y%m%d%H%M%S.%f")}
        mock_datetime_mgmt.convert_string_to_date.return_value = timestamp

        # Mock de extract_string_after_slash
        mock_extract_string.return_value = "RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de insert_rta_procesamiento
        mock_insert_rta_procesamiento.return_value = ("query_insert", "params_insert")

        # Mock de postgres_service.get_all
        self.mock_postgres_service.get_all.return_value = (
            [{"estado": "INICIAL"}],  # Result
            None,  # Error
            None   # Description
        )

        # Mock de postgres_service.insert
        self.mock_postgres_service.insert.return_value = (None, None, None)

        # Mock de postgres_service.update_by_id
        self.mock_postgres_service.update_by_id.return_value = (None, None, None)

        # Mock de postgres_service.query devuelve error
        self.mock_postgres_service.query.return_value = (None, True, "Query error")

        # Act
        self.actions.process_update_db(id_archivo, file_name)

        # Assert
        # Verificar que se registró el error
        self.mock_logger_service.log_error.assert_called_with("Query error")

        # Verificar que se llamó a process_file_error
        self.mock_error_handling.process_file_error.assert_called_once()

    @patch('src.core.actions.insert_rta_procesamiento')
    @patch('src.core.actions.extract_string_after_slash')
    @patch('src.core.actions.DatetimeManagement')
    def test_process_update_db_tipo_respuesta(self, mock_datetime_mgmt, mock_extract_string, mock_insert_rta_procesamiento):
        # Arrange
        id_archivo = 1
        test_cases = [
            ("Recibidos/RE_PRO_TUTGMF0001003920240930-0001-R.zip", "02"),
            ("Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip", "01"),
            ("Recibidos/RE_ESP_TUTGMF0001003920240930-0001.zip", "03"),
            ("Recibidos/OTHER_TUTGMF0001003920240930-0001.zip", ""),
        ]

        # Mock de DatetimeManagement
        timestamp = datetime.now()
        mock_datetime_mgmt.get_datetime.return_value = {"timestamp": timestamp.strftime("%Y%m%d%H%M%S.%f")}
        mock_datetime_mgmt.convert_string_to_date.return_value = timestamp

        for file_name, expected_tipo_respuesta in test_cases:
            with self.subTest(file_name=file_name, expected_tipo_respuesta=expected_tipo_respuesta):
                # Resetear los mocks
                self.mock_postgres_service.get_all.reset_mock()
                self.mock_postgres_service.insert.reset_mock()
                self.mock_postgres_service.update_by_id.reset_mock()
                self.mock_postgres_service.query.reset_mock()
                self.mock_logger_service.reset_mock()
                self.mock_error_handling.reset_mock()

                # Mock de extract_string_after_slash
                mock_extract_string.return_value = extract_string_after_slash(file_name)

                # Mock de insert_rta_procesamiento
                mock_insert_rta_procesamiento.return_value = ("query_insert", "params_insert")

                # Mock de postgres_service.get_all
                self.mock_postgres_service.get_all.return_value = (
                    [{"estado": "INICIAL"}],  # Result
                    None,  # Error
                    None   # Description
                )

                # Mock de postgres_service.insert
                self.mock_postgres_service.insert.return_value = (None, None, None)

                # Mock de postgres_service.update_by_id
                self.mock_postgres_service.update_by_id.return_value = (None, None, None)

                # Mock de postgres_service.query
                self.mock_postgres_service.query.return_value = (None, None, None)

                # Act
                self.actions.process_update_db(id_archivo, file_name)

                # Assert
                # Verificar que se llamó a insert_rta_procesamiento con el tipo_respuesta correcto
                mock_insert_rta_procesamiento.assert_called_with(
                    id_archivo,
                    extract_string_after_slash(file_name),
                    expected_tipo_respuesta
                )

    @patch.object(Actions, 'process_update_db')
    @patch('src.core.actions.extract_string_after_slash')
    def test_process_file_and_update_db_success(self, mock_extract_string, mock_process_update_db):
        # Arrange
        id_archivo = 1
        file_name = "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip"
        nombre_archivo_destino = "RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de extract_string_after_slash
        mock_extract_string.return_value = nombre_archivo_destino

        # Mock de s3_service.move_file para que retorne True (éxito)
        self.mock_s3_service.move_file.return_value = True

        # Act
        self.actions.process_file_and_update_db(id_archivo, file_name)

        # Assert
        # Verificar que se llamó a extract_string_after_slash con file_name
        mock_extract_string.assert_called_once_with(file_name)

        # Verificar que se llamó a s3_service.move_file con los parámetros correctos
        self.mock_s3_service.move_file.assert_called_once_with(
            self.mock_env.BUCKET,
            file_name,
            self.mock_env.FOLDER_PROCESSING,
            nombre_archivo_destino
        )

        # Verificar que no se llamó a log_error ni a process_file_error
        self.mock_logger_service.log_error.assert_not_called()
        self.mock_error_handling.process_file_error.assert_not_called()

        # Verificar que se llamó a process_update_db con los parámetros correctos
        mock_process_update_db.assert_called_once_with(id_archivo, file_name)

    @patch.object(Actions, 'process_update_db')
    @patch('src.core.actions.extract_string_after_slash')
    def test_process_file_and_update_db_move_failed(self, mock_extract_string, mock_process_update_db):
        # Arrange
        id_archivo = 1
        file_name = "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip"
        nombre_archivo_destino = "RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de extract_string_after_slash
        mock_extract_string.return_value = nombre_archivo_destino

        # Mock de s3_service.move_file para que retorne False (falla)
        self.mock_s3_service.move_file.return_value = False

        # Act
        self.actions.process_file_and_update_db(id_archivo, file_name)

        # Assert
        # Verificar que se llamó a extract_string_after_slash con file_name
        mock_extract_string.assert_called_once_with(file_name)

        # Verificar que se llamó a s3_service.move_file con los parámetros correctos
        self.mock_s3_service.move_file.assert_called_once_with(
            self.mock_env.BUCKET,
            file_name,
            self.mock_env.FOLDER_PROCESSING,
            nombre_archivo_destino
        )

        # Verificar que se llamó a log_error con el mensaje correcto
        self.mock_logger_service.log_error.assert_called_once_with("No se pudo mover a la carpeta procesando")

        # Verificar que se llamó a process_file_error con los parámetros correctos
        self.mock_error_handling.process_file_error.assert_called_once_with(
            updates={
                "error_code": "EICP006",
                "error_detail": "No se pudo mover a la carpeta procesando",
            },
            file_id=id_archivo,
            move_file=False
        )

        # Verificar que no se llamó a process_update_db
        mock_process_update_db.assert_not_called()

    @patch('src.core.actions.Unzipfile')
    @patch.object(Actions, 'validate_states')
    @patch.object(Actions, 'process_file_and_update_db')
    @patch.object(Actions, 'check_unzipped_files')
    @patch.object(Actions, 'rejected_state_errors')
    def test_normal_flow_state_validation_needed(self, mock_rejected_state_errors, mock_check_unzipped_files, mock_process_file_and_update_db, mock_validate_states, mock_unzipfile_class):
        # Arrange
        file_data = {
            "file_id": 1,
            "state": "INICIAL",
            "file_name": "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip",
            "result_state_validation": None,
            "result_zip_validation": None,
            "result_file_validation": None
        }

        id_archivo = file_data["file_id"]
        estado = file_data["state"]
        file_name = file_data["file_name"]

        # Mock de validate_states para que retorne True
        mock_validate_states.return_value = True

        # Mock de Unzipfile y su método unzip_file_data
        mock_unzipfile_instance = MagicMock()
        mock_unzipfile_class.return_value = mock_unzipfile_instance
        mock_unzipfile_instance.unzip_file_data.return_value = (True, 'unzipped_folder_name')

        # Act
        self.actions.normal_flow(file_data)

        # Assert
        # Verificar que validate_states fue llamado
        mock_validate_states.assert_called_once_with(id_archivo, estado, file_name)

        # Verificar que process_file_and_update_db fue llamado
        mock_process_file_and_update_db.assert_called_once_with(id_archivo, file_name)

        # Verificar que unzip_file_data fue llamado
        mock_unzipfile_instance.unzip_file_data.assert_called_once_with(
            self.mock_env.BUCKET,
            self.mock_env.FOLDER_PROCESSING,
           ANY,
        )

        # Verificar que check_unzipped_files fue llamado
        mock_check_unzipped_files.assert_called_once_with('unzipped_folder_name', file_data)

        # Verificar que rejected_state_errors no fue llamado
        mock_rejected_state_errors.assert_not_called()

    @patch.object(Actions, 'validate_states')
    @patch.object(Actions, 'process_file_and_update_db')
    @patch.object(Actions, 'check_unzipped_files')
    @patch.object(Actions, 'rejected_state_errors')
    def test_normal_flow_state_validation_failed(self, mock_rejected_state_errors, mock_check_unzipped_files, mock_process_file_and_update_db, mock_validate_states):
        # Arrange
        file_data = {
            "file_id": 1,
            "state": "INICIAL",
            "file_name": "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip",
            "result_state_validation": None,
            "result_zip_validation": None,
            "result_file_validation": None
        }

        id_archivo = file_data["file_id"]
        estado = file_data["state"]
        file_name = file_data["file_name"]

        # Mock de validate_states para que retorne False
        mock_validate_states.return_value = False

        # Act
        self.actions.normal_flow(file_data)

        # Assert
        # Verificar que validate_states fue llamado
        mock_validate_states.assert_called_once_with(id_archivo, estado, file_name)

        # Verificar que no se llamó a otros métodos
        mock_process_file_and_update_db.assert_not_called()
        mock_check_unzipped_files.assert_not_called()
        mock_rejected_state_errors.assert_not_called()
        

    @patch('src.core.actions.Unzipfile')
    @patch.object(Actions, 'process_file_and_update_db')
    @patch.object(Actions, 'check_unzipped_files')
    @patch.object(Actions, 'rejected_state_errors')
    def test_normal_flow_zip_validation_needed(self, mock_rejected_state_errors, mock_check_unzipped_files, mock_process_file_and_update_db, mock_unzipfile_class):
        # Arrange
        file_data = {
            "file_id": 1,
            "state": "INICIAL",
            "file_name": "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip",
            "result_state_validation": True,
            "result_zip_validation": None,
            "result_file_validation": None
        }

        id_archivo = file_data["file_id"]
        file_name = file_data["file_name"]

        # Mock de Unzipfile y su método unzip_file_data
        mock_unzipfile_instance = MagicMock()
        mock_unzipfile_class.return_value = mock_unzipfile_instance
        mock_unzipfile_instance.unzip_file_data.return_value = (True, 'unzipped_folder_name')

        # Act
        self.actions.normal_flow(file_data)

        # Assert
        # Verificar que process_file_and_update_db fue llamado
        mock_process_file_and_update_db.assert_called_once_with(id_archivo, file_name)

        # Verificar que unzip_file_data fue llamado
        mock_unzipfile_instance.unzip_file_data.assert_called_once_with(
            self.mock_env.BUCKET,
            self.mock_env.FOLDER_PROCESSING,
           ANY,
        )

        # Verificar que check_unzipped_files fue llamado
        mock_check_unzipped_files.assert_called_once_with('unzipped_folder_name', file_data)

        # Verificar que rejected_state_errors no fue llamado
        mock_rejected_state_errors.assert_not_called()
        
        
        
        
    @patch('src.core.actions.extract_string_after_slash')
    @patch('src.core.actions.insert_rta_pro_archivos')
    @patch('src.core.actions.extract_text_type')
    @patch('src.core.actions.Unzipfile')
    @patch.object(Actions, 'rejected_state_errors')
    def test_validate_files_and_register_indb_success(self, mock_rejected_state_errors, mock_unzipfile_class, mock_extract_text_type, mock_insert_rta_pro_archivos, mock_extract_string):
        # Arrange
        file_date = {
            "id_archivo": 1,
            "file_name": "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip",
            "estado": "ENVIADO",
            "result": True,
            "valido": True,
            "todos_comienzan_con_re": True,
            "coincidencias": True,
            "archivos": ["RE_TUTGMF0001003920240930-0001-CONTROLTX.txt",
                         "RE_TUTGMF0001003920240930-0001-INCONSISTENCIASPROC.txt",
                        ]
        }

        id_archivo = file_date["id_archivo"]
        file_name = file_date["file_name"]
        estado = file_date["estado"]

        # Mock de extract_string_after_slash
        mock_extract_string.return_value = "RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de Unzipfile y su método read_s3
        mock_unzipfile_instance = MagicMock()
        mock_unzipfile_class.return_value = mock_unzipfile_instance
        mock_unzipfile_instance.read_s3.return_value = ("query", "path/")

        # Mock de insert_rta_pro_archivos
        mock_insert_rta_pro_archivos.return_value = ("query_insert", "params_insert")

        # Mock de extract_text_type
        mock_extract_text_type.return_value = "text_type"

        # Mock de postgres_service.query
        self.mock_postgres_service.query.return_value = (None, None, None)

        # Mock de postgres_service.update_all
        self.mock_postgres_service.update_all.return_value = (None, None, None)

        # Act
        result = self.actions.validate_files_and_register_indb(file_date)

        # Assert
        self.assertTrue( result)
        # Verificar que se llamó a read_s3
        mock_unzipfile_instance.read_s3.assert_called_once_with(
            self.mock_env.BUCKET,
            self.mock_env.FOLDER_PROCESSING,
            "RE_PRO_TUTGMF0001003920240930-0001"
        )
        # Verificar que se llamó a insert_rta_pro_archivos para cada archivo
        self.assertEqual(mock_insert_rta_pro_archivos.call_count, 2)
        # Verificar que se envió mensaje a SQS por cada archivo
        self.assertEqual(self.mock_sqs_service.send_message.call_count, 2)
        # Verificar que se llamó a update_all
        self.mock_postgres_service.update_all.assert_called_once()
        # Verificar que las llamadas a process_file_error fueron con los argumentos correctos
        self.mock_error_handling.process_file_error.assert_has_calls([
            call(
                updates={"error_code": "EICP006", "error_detail": None},
                file_id=id_archivo,
                move_file=False
            ),
            call(
                updates={"error_code": "EICP006", "error_detail": None},
                file_id=id_archivo,
                move_file=False
            )
        ])
        # Verificar que no se llamó a rejected_state_errors ni a process_file_error
        mock_rejected_state_errors.assert_not_called()


    @patch('src.core.actions.extract_string_after_slash')
    @patch.object(Actions, 'rejected_state_errors')
    @patch('src.core.actions.Unzipfile')
    def test_validate_files_and_register_indb_conditions_not_met(self, mock_unzipfile_class, mock_rejected_state_errors, mock_extract_string):
        # Arrange
        file_date = {
            "id_archivo": 1,
            "file_name": "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip",
            "estado": "INICIAL",
            "result": True,
            "valido": False,  # Condición no cumplida
            "todos_comienzan_con_re": True,
            "coincidencias": True,
            "archivos": ["archivo1.txt", "archivo2.txt"]
        }

        id_archivo = file_date["id_archivo"]
        file_name = file_date["file_name"]
        estado = file_date["estado"]

        # Mock de extract_string_after_slash
        mock_extract_string.return_value = "RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de Unzipfile y su método read_s3
        mock_unzipfile_instance = MagicMock()
        mock_unzipfile_class.return_value = mock_unzipfile_instance
        mock_unzipfile_instance.read_s3.return_value = ("query", "path/")

        # Act
        result = self.actions.validate_files_and_register_indb(file_date)

        # Assert
        self.assertFalse(result)
        # Verificar que se llamó a rejected_state_errors con el código "EICP005"
        mock_rejected_state_errors.assert_called_once_with(
            id_archivo, file_name, estado, "EICP005"
        )
        # Verificar que no se llamó a process_file_error
        self.mock_error_handling.process_file_error.assert_not_called()

    @patch('src.core.actions.extract_string_after_slash')
    @patch.object(Actions, 'rejected_state_errors')
    @patch('src.core.actions.Unzipfile')
    def test_validate_files_and_register_indb_file_check_failed(self, mock_unzipfile_class, mock_rejected_state_errors, mock_extract_string):
        # Arrange
        file_date = {
            "id_archivo": 1,
            "file_name": "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip",
            "estado": "INICIAL",
            "result": True,
            "valido": True,
            "todos_comienzan_con_re": True,
            "coincidencias": True,
            "archivos": ["archivo1.txt", "archivo2.txt"]
        }

        id_archivo = file_date["id_archivo"]
        file_name = file_date["file_name"]
        estado = file_date["estado"]

        # Mock de extract_string_after_slash
        # Ajustamos el retorno para que falle la condición
        def side_effect_extract_string(s):
            if s == file_name:
                return "RE_PRO_TUTGMF0001003920240930-0001.zip"
            else:
                return s

        mock_extract_string.side_effect = side_effect_extract_string

        # Mock de Unzipfile y su método read_s3
        mock_unzipfile_instance = MagicMock()
        mock_unzipfile_class.return_value = mock_unzipfile_instance
        mock_unzipfile_instance.read_s3.return_value = ("query", "path/")

        # Act
        result = self.actions.validate_files_and_register_indb(file_date)

        # Assert
        self.assertFalse(result)
        # Verificar que se llamó a rejected_state_errors con el código "EICP004"
        mock_rejected_state_errors.assert_called_once_with(
            id_archivo, file_name, estado, "EICP004"
        )
        # Verificar que no se llamó a process_file_error
        self.mock_error_handling.process_file_error.assert_not_called()

    @patch('src.core.actions.extract_string_after_slash')
    @patch('src.core.actions.insert_rta_pro_archivos')
    @patch('src.core.actions.extract_text_type')
    @patch.object(Actions, 'rejected_state_errors')
    @patch('src.core.actions.Unzipfile')
    def test_validate_files_and_register_indb_query_error(self, mock_unzipfile_class, mock_rejected_state_errors, mock_extract_text_type, mock_insert_rta_pro_archivos, mock_extract_string):
        # Arrange
        file_date = {
            "id_archivo": 1,
            "file_name": "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip",
            "estado": "INICIAL",
            "result": True,
            "valido": True,
            "todos_comienzan_con_re": True,
            "coincidencias": True,
            "archivos": ["RE_TUTGMF0001003920240930-0001-CONTROLTX.txt"]
        }

        id_archivo = file_date["id_archivo"]
        file_name = file_date["file_name"]
        estado = file_date["estado"]

        # Mock de extract_string_after_slash
        mock_extract_string.return_value = "RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de Unzipfile y su método read_s3
        mock_unzipfile_instance = MagicMock()
        mock_unzipfile_class.return_value = mock_unzipfile_instance
        mock_unzipfile_instance.read_s3.return_value = ("query", "path/")

        # Mock de insert_rta_pro_archivos
        mock_insert_rta_pro_archivos.return_value = ("query_insert", "params_insert")

        # Mock de extract_text_type
        mock_extract_text_type.return_value = "text_type"

        # Mock de postgres_service.query para que devuelva un error
        self.mock_postgres_service.query.return_value = (None, True, "Error in query")

        # Act
        result = self.actions.validate_files_and_register_indb(file_date)

        # Assert
        self.assertEqual(result, (id_archivo, True, "Error in query"))
        # Verificar que se llamó a process_file_error
        self.mock_error_handling.process_file_error.assert_called_once_with(
            updates={
                "error_code": "EICP006",
                "error_detail": "Error in query",
            },
            file_id=id_archivo,
            move_file=False
        )
        # Verificar que no se llamó a rejected_state_errors
        mock_rejected_state_errors.assert_not_called()

    @patch('src.core.actions.extract_string_after_slash')
    @patch.object(Actions, 'rejected_state_errors')
    @patch('src.core.actions.Unzipfile')
    def test_validate_files_and_register_indb_update_all_error(self, mock_unzipfile_class, mock_rejected_state_errors, mock_extract_string):
        # Arrange
        file_date = {
            "id_archivo": 1,
            "file_name": "Recibidos/RE_PRO_TUTGMF0001003920240930-0001.zip",
            "estado": "INICIAL",
            "result": True,
            "valido": True,
            "todos_comienzan_con_re": True,
            "coincidencias": True,
            "archivos": ["RE_TUTGMF0001003920240930-0001-CONTROLTX.txt"]
        }

        id_archivo = file_date["id_archivo"]
        file_name = file_date["file_name"]
        estado = file_date["estado"]

        # Mock de extract_string_after_slash
        mock_extract_string.return_value = "RE_PRO_TUTGMF0001003920240930-0001.zip"

        # Mock de Unzipfile y su método read_s3
        mock_unzipfile_instance = MagicMock()
        mock_unzipfile_class.return_value = mock_unzipfile_instance
        mock_unzipfile_instance.read_s3.return_value = ("query", "path/")

        # Mock de postgres_service.query
        self.mock_postgres_service.query.return_value = (None, None, None)

        # Mock de postgres_service.update_all para que devuelva un error
        self.mock_postgres_service.update_all.return_value = (None, True, None)

        # Act
        result = self.actions.validate_files_and_register_indb(file_date)

        # Assert
        self.assertEqual(result, (id_archivo, True, None))
        # Verificar que se llamó a process_file_error
        self.mock_error_handling.process_file_error.assert_has_calls([
            call(
                updates={
                    "error_code": "EICP006",
                    "error_detail": None,
                },
                file_id=id_archivo,
                move_file=False
            ),
            call(
                updates={
                    "error_code": "EICP006",
                    "error_detail": None,
                },
                file_id=id_archivo,
                move_file=False
            )
        ])
        # Verificar que no se llamó a rejected_state_errors
        mock_rejected_state_errors.assert_not_called()        
        
if __name__ == "__main__":
    unittest.main()
