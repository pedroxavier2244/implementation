import io

try:
    import boto3
    from botocore.client import Config
except ModuleNotFoundError:
    boto3 = None  # type: ignore[assignment]
    Config = None  # type: ignore[assignment]

from shared.config import get_settings


class MinioClient:
    def __init__(self):
        if boto3 is None or Config is None:
            raise RuntimeError("boto3 is required to use MinioClient")

        settings = get_settings()
        self.bucket = settings.MINIO_BUCKET
        scheme = "https" if settings.MINIO_SECURE else "http"
        self._client = boto3.client(
            "s3",
            endpoint_url=f"{scheme}://{settings.MINIO_ENDPOINT}",
            aws_access_key_id=settings.MINIO_ACCESS_KEY,
            aws_secret_access_key=settings.MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        self._ensure_bucket()

    def _ensure_bucket(self):
        try:
            self._client.head_bucket(Bucket=self.bucket)
        except Exception:
            self._client.create_bucket(Bucket=self.bucket)

    def upload_file(self, file_bytes: bytes, object_name: str) -> str:
        self._client.put_object(
            Bucket=self.bucket,
            Key=object_name,
            Body=io.BytesIO(file_bytes),
            ContentLength=len(file_bytes),
        )
        return object_name

    def download_file(self, object_name: str) -> bytes:
        response = self._client.get_object(Bucket=self.bucket, Key=object_name)
        return response["Body"].read()

    def object_exists(self, object_name: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=object_name)
            return True
        except Exception:
            return False
