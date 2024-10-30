import unittest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from src.services.s3_service import S3Service
from src.services.logger_service import LoggerService
from src.utils.environment import Environment

class TestS3Service(unittest.TestCase):
    """Clase para el manejo de tests de S3Service"""

    def setUp(self):
        self.mock_env = MagicMock(spec=Environment)
        self.mock_env.REGION_ZONE = 'us-east-1'
        self.mock_env.LOCALSTACK_ENDPOINT = 'http://localhost:4566'
        self.mock_env.IS_LOCAL = True

        self.mock_logger_service = MagicMock(spec=LoggerService)

        self.s3_service = S3Service(
            env=self.mock_env, logger_service=self.mock_logger_service
        )
        self.s3_service.client = MagicMock()

    def tearDown(self):
        self.s3_service.client.reset_mock()
        self.mock_env.reset_mock()
        self.mock_logger_service.reset_mock()

    def test_read_file_with_blocks(self):
        mock_body = MagicMock()
        mock_body.iter_lines.return_value = [b"line1", b"line2", b"line3"]
        self.s3_service.client.get_object.return_value = {"Body": mock_body}

        result, total_records, error = self.s3_service.read_file(
            "test-bucket", "test-object", blocks=2
        )

        self.assertEqual(result, [["line1", "line2"], ["line3"]])
        self.assertEqual(total_records, 3)
        self.assertFalse(error)

    def test_read_file_error(self):
        self.s3_service.client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "get_object"
        )

        result, total_records, error = self.s3_service.read_file("test-bucket", "test-object")

        self.assertEqual(result, "")
        self.assertEqual(total_records, 0)
        self.assertTrue(error)

    def test_download_file(self):
        result = self.s3_service.download_file("test-bucket", "test-object", "local-file")

        self.assertFalse(result)
        self.s3_service.client.download_file.assert_called_once_with(
            "test-bucket", "test-object", "local-file"
        )

    def test_download_file_error(self):
        self.s3_service.client.download_file.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "download_file"
        )

        result = self.s3_service.download_file("test-bucket", "test-object", "local-file")

        self.assertTrue(result)

    def test_upload_file(self):
        result = self.s3_service.upload_file("local-file", "test-bucket", "test-object")

        self.assertFalse(result)
        self.s3_service.client.upload_file.assert_called_once_with(
            "local-file",
            "test-bucket",
            "test-object"
        )

    def test_upload_file_error(self):
        self.s3_service.client.upload_file.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}},
            "upload_file"
        )

        result = self.s3_service.upload_file("local-file", "test-bucket", "test-object")

        self.assertTrue(result)

    def test_create_file(self):
        result = self.s3_service.create_file("test-bucket", "test-object", "content")

        self.assertFalse(result)
        self.s3_service.client.put_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="test-object",
            Body="content"
        )

    def test_create_file_error(self):
        self.s3_service.client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}},
            "put_object"
        )

        result = self.s3_service.create_file("test-bucket", "test-object", "content")

        self.assertTrue(result)

    def test_delete_file_error(self):
        self.s3_service.client.delete_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "delete_object"
        )

        result = self.s3_service.delete_file("test-bucket", "test-object")

        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()
