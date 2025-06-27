from __future__ import annotations

import io
import typing
from contextlib import asynccontextmanager
from os import PathLike

import aioboto3
from botocore.config import Config
from botocore.errorfactory import ClientError

from .exceptions import ObjectDoesNotExistError
from .settings import settings

if typing.TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_s3.type_defs import CopySourceTypeDef, GetObjectOutputTypeDef


class S3Storage:
    """Base storage class."""

    access_key_id = settings.access_key_id
    secret_access_key = settings.secret_access_key
    use_ssl = settings.use_ssl
    verify = settings.verify
    endpoint_url = settings.endpoint_url
    config = Config(
        s3={"addressing_style": settings.addressing_style},
        signature_version=settings.signature_version,
        proxies=settings.proxies,
        connect_timeout=settings.connect_timeout,
        read_timeout=settings.read_timeout,
    )
    bucket_name: str = ""
    location: str = ""

    async def open(self, name: str) -> typing.BinaryIO:
        """Open file method.

        :param name: Filename or path file.
        :return: File.
        """
        return await self._open(name)

    async def save(self, name: str, content: typing.BinaryIO, extra_args: dict) -> None:
        """Save file to storage.

        :param name: Filename or path file.
        :param content: Content file.
        """
        if not name:
            name = content.name
        await self._save(name, content, extra_args=extra_args)

    @property
    def session(self) -> aioboto3.Session:
        """The session stores the configuration state and allows the client to be created.

        :return: aioboto3.Session.
        """
        return aioboto3.Session(aws_access_key_id=self.access_key_id, aws_secret_access_key=self.secret_access_key)

    def client(self) -> S3Client:
        """Client to interact with s3.

        :return: object S3Client.
        """
        return self.session.client(
            service_name="s3",
            use_ssl=self.use_ssl,
            verify=self.verify,
            endpoint_url=self.endpoint_url,
            config=self.config,
        )

    def _get_full_key(self, name: str | PathLike[str]) -> str:
        """Get path key.

        :param name: Filename or path file.
        :return: Full path file.
        """
        if self.location:
            return f"{self.location}/{name}"
        return str(name)

    @asynccontextmanager
    async def get_object(self, path: str | PathLike[str]) -> AsyncGenerator[GetObjectOutputTypeDef, None]:
        async with self.client() as s3:
            try:
                yield await s3.get_object(Bucket=self.bucket_name, Key=self._get_full_key(path))
            except ClientError as e:
                if "NoSuchKey" in e.args[0]:
                    raise ObjectDoesNotExistError(f"File with path {path} does not exist.") from e

    async def _save(self, name: str, content: typing.BinaryIO, extra_args: dict) -> None:
        async with self.client() as client:
            await client.upload_fileobj(
                Fileobj=content, Bucket=self.bucket_name, Key=self._get_full_key(name=name), ExtraArgs=extra_args
            )

    async def _open(self, name: str) -> typing.BinaryIO:
        async with self.client() as client:
            try:
                response = await client.get_object(Bucket=self.bucket_name, Key=self._get_full_key(name=name))
            except ClientError as error:
                if error.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                    raise FileNotFoundError(f"File does not exist: {name}") from error
                raise  # If there are other errors, let it fall out.
            return io.BytesIO(await response["Body"].read())

    async def delete(self, name: str) -> None:
        """Remove a file from storage.

        :param name: Filename or path file.
        """
        async with self.client() as client:
            await client.delete_object(Bucket=self.bucket_name, Key=self._get_full_key(name=name))

    async def copy(self, copy_source: CopySourceTypeDef, bucket: str, key: str, **kwargs):
        async with self.client() as client:
            await client.copy(CopySource=copy_source, Bucket=bucket, Key=key, **kwargs)

    async def generate_presigned_url(
        self,
        key: str,
        client_method: str = "get_object",
        expires_in: int = 3600,
    ) -> str:
        params = {"Bucket": self.bucket_name, "Key": self._get_full_key(key)}
        async with self.client() as client:
            return await client.generate_presigned_url(
                Params=params,
                ClientMethod=client_method,
                ExpiresIn=expires_in,
            )
