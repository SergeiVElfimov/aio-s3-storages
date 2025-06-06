from __future__ import annotations

from http import HTTPStatus
from typing import Annotated
from urllib.parse import quote

import pytest
from fastapi import Depends, FastAPI

from fastapi_s3_storages.base import S3Storage
from fastapi_s3_storages.responses import FileStreamingResponse

FILE_NAME = "test file.abc"


app = FastAPI()


@app.get("/download/{file_path:path}")
async def download(file_path: str, storage: Annotated[S3Storage, Depends()]):
    return FileStreamingResponse(storage, path=file_path, filename=FILE_NAME)


@pytest.fixture
def web_app(s3_storage_client_mock) -> FastAPI:
    app.dependency_overrides[S3Storage] = s3_storage_client_mock
    return app


class TestFileStreamingResponse:
    async def test_ok(self, create_object, http_client) -> None:
        file_key = "some-dir/some-file.abc"
        content = b"<file content>" * 10
        await create_object(file_key, body=content)
        expected_disposition = f"attachment; filename*=utf-8''{quote(FILE_NAME)}"

        url = app.url_path_for("download", file_path=file_key)
        response = await http_client.get(url)

        assert response.status_code == HTTPStatus.OK
        assert response.content == content
        assert response.headers["content-type"] == "binary/octet-stream"
        assert response.headers["content-disposition"] == expected_disposition
        assert "content-length" in response.headers
        assert "last-modified" in response.headers
        assert "etag" in response.headers

    async def test_not_found(self, http_client) -> None:
        file_key = "some-dir/some-file.abc"

        url = app.url_path_for("download", file_path=file_key)
        response = await http_client.get(url)

        assert response.status_code == HTTPStatus.NOT_FOUND
