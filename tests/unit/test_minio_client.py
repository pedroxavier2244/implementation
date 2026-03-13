from unittest.mock import MagicMock, patch


def test_upload_file_returns_path():
    file_content = b"test content"
    with patch("shared.minio_client.boto3") as mock_boto3:
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_bucket.return_value = {}

        from shared.minio_client import MinioClient

        client = MinioClient.__new__(MinioClient)
        client._client = mock_client
        client.bucket = "test-bucket"

        path = client.upload_file(file_bytes=file_content, object_name="2026/02/27/test.xlsx")
        assert path == "2026/02/27/test.xlsx"
        mock_client.put_object.assert_called_once()


def test_download_file_returns_bytes():
    with patch("shared.minio_client.boto3") as mock_boto3:
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_body = MagicMock()
        mock_body.read.return_value = b"file bytes"
        mock_client.get_object.return_value = {"Body": mock_body}

        from shared.minio_client import MinioClient

        client = MinioClient.__new__(MinioClient)
        client._client = mock_client
        client.bucket = "test-bucket"

        result = client.download_file("some/path.xlsx")
        assert result == b"file bytes"
