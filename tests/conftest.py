from __future__ import annotations

import typing

import pytest

from aio_s3_storages import S3Storage

pytest_plugins = ("tests.pytest.plugin",)

if typing.TYPE_CHECKING:
    from pytest_mock import MockerFixture
    from types_aiobotocore_s3.client import S3Client


@pytest.fixture
def s3_storage_client_mock(s3_client: S3Client, bucket_name: str, mocker: MockerFixture) -> type[S3Storage]:
    client_mock = mocker.AsyncMock()
    client_mock.__aenter__.return_value = s3_client
    mocker.patch.object(S3Storage, "client", mocker.MagicMock(return_value=client_mock))
    S3Storage.bucket_name = bucket_name
    return S3Storage
