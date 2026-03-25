import io
import logging

logger = logging.getLogger(__name__)

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
            logger.debug("Bucket '%s' already exists", self.bucket)
        except Exception as exc:
            logger.warning("Bucket '%s' not found, creating: %s", self.bucket, exc)
            try:
                self._client.create_bucket(Bucket=self.bucket)
                logger.info("Bucket '%s' created successfully", self.bucket)
            except Exception as create_exc:
                logger.error("Failed to create bucket '%s': %s", self.bucket, create_exc)
                raise

    def upload_file(self, file_bytes: bytes, object_name: str) -> str:
        logger.debug("Uploading %d bytes to '%s/%s'", len(file_bytes), self.bucket, object_name)
        self._client.put_object(
            Bucket=self.bucket,
            Key=object_name,
            Body=io.BytesIO(file_bytes),
            ContentLength=len(file_bytes),
        )
        logger.info("Uploaded '%s' (%d bytes)", object_name, len(file_bytes))
        return object_name

    def download_file(self, object_name: str) -> bytes:
        logger.debug("Downloading '%s/%s'", self.bucket, object_name)
        response = self._client.get_object(Bucket=self.bucket, Key=object_name)
        data = response["Body"].read()
        logger.info("Downloaded '%s' (%d bytes)", object_name, len(data))
        return data

    def object_exists(self, object_name: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=object_name)
            return True
        except Exception as exc:
            logger.debug("Object '%s' not found: %s", object_name, exc)
            return False
