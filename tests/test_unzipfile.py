import unittest
from unittest.mock import patch, MagicMock
from src.core.unzip_file import Unzipfile
import os


class TestUnzipfile(unittest.TestCase):

    def setUp(self):
        # Mock services and error handling
        self.mock_services = {
            'env': MagicMock(),  
            'logger_service': MagicMock(), 
        }
        self.mock_error_handling = MagicMock() 

        # Instantiate the Unzipfile class
        self.unzip_file_instance = Unzipfile(
            services=self.mock_services,
            error_handling=self.mock_error_handling
        )

    @patch('boto3.client')
    @patch('os.makedirs')
    @patch('os.remove')
    @patch('zipfile.ZipFile')
    def test_unzip_file_data_success(
        self, mock_zipfile, mock_remove, mock_makedirs, mock_boto_client
    ):
        # Mocks for boto3 S3
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Mock the response from S3 listing objects
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'path/folder/test.zip'}]
        }

        # Mock the download of the file
        mock_s3.download_file.return_value = None

        # Simulate extracting files with zipfile
        mock_zip = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip

        # Mock uploading files to S3
        mock_s3.upload_fileobj.return_value = None

        # Execute the method to test
        result, unzipped_folder_name = self.unzip_file_instance.unzip_file_data(
            bucket_name='test-bucket', folder_name='test-folder/', project_root='/tmp/'
        )

        # Assertions
        self.assertTrue(result)
        self.assertTrue(unzipped_folder_name.startswith('test'))

        # Check that files were downloaded and extracted
        mock_s3.download_file.assert_called_once_with(
            'test-bucket', 'path/folder/test.zip', '/tmp/test.zip'
        )
        mock_zip.extractall.assert_called_once()

    @patch('boto3.client')
    @patch('os.makedirs')
    @patch('shutil.rmtree')
    def test_unzip_file_data_exception_handling(
        self, mock_rmtree, mock_makedirs, mock_boto_client
    ):
        # Mocks for boto3 S3
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Mock the response from S3, no zip file
        mock_s3.list_objects_v2.return_value = {'Contents': []}

        # Execute the method, expecting it to raise an error
        result, unzipped_folder_name = self.unzip_file_instance.unzip_file_data(
            bucket_name='test-bucket', folder_name='test-folder/', project_root='/tmp/'
        )

        # Assertions for failure
        self.assertFalse(result)
        self.assertEqual(unzipped_folder_name, '')

        # Assert that the logger captured the error
        self.mock_services['logger_service'].log_error.assert_called_with(
            'Error: No zip file found in folder test-folder/.'
        )

    @patch('boto3.client')
    def test_read_s3_success(self, mock_boto_client):
        # Mock boto3 client
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Mock list_objects_v2 response
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'test-folder/RE_ESP_file1_20240921230444/'},
                {'Key': 'test-folder/RE_ESP_file2_20240921230445/'}
            ]
        }

        # Execute the method
        files, folder = self.unzip_file_instance.read_s3(
            bucket_name='test-bucket', folder_name='test-folder/', file_name='RE_ESP'
        )

        # Assertions
        self.assertEqual(folder, 'RE_ESP_file2_20240921230445')
        self.assertEqual(len(files), 2)

    @patch('boto3.resource')
    def test_move_folder(self, mock_boto_resource):
        # Mock boto3 S3 resource
        mock_s3 = MagicMock()
        mock_boto_resource.return_value = mock_s3

        # Mock the S3 bucket and its objects
        mock_bucket = MagicMock()
        mock_s3.Bucket.return_value = mock_bucket
        mock_bucket.objects.filter.return_value = [
            MagicMock(key='source-folder/file1.txt'),
            MagicMock(key='source-folder/file2.txt')
        ]

        # Execute the method
        self.unzip_file_instance.move_folder(
            bucket_name='test-bucket', source_folder='source-folder/', destination_folder='dest-folder/'
        )

        # Assertions to check that files are copied and deleted
        mock_s3.Object().copy_from.assert_called()
        mock_s3.Object().delete.assert_called()

    @patch('boto3.client')
    @patch('os.remove')
    @patch('os.path.exists')
    def test_finally_cleanup(self, mock_exists, mock_remove, mock_boto_client):
        # Mock boto3 client and remove
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Mock the response from S3 listing objects
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'path/folder/test.zip'}]
        }

        # Simulate that the file exists, so os.remove is called
        mock_exists.return_value = True

        # Execute the method to trigger the finally block
        result, unzipped_folder_name = self.unzip_file_instance.unzip_file_data(
            bucket_name='test-bucket', folder_name='test-folder/', project_root='/tmp/'
        )

        # Check that the cleanup was called
        mock_remove.assert_called_once_with('/tmp/test.zip')


if __name__ == '__main__':
    unittest.main()
