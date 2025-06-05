from __future__ import annotations

import os
import typing

from fastapi import HTTPException, status
from fastapi.responses import FileResponse
from starlette.types import Receive, Scope, Send

from .base import S3Storage
from .exceptions import ObjectDoesNotExistError

if typing.TYPE_CHECKING:
    from types_aiobotocore_s3.type_defs import GetObjectOutputTypeDef

TypingS3Storage = typing.TypeVar("TypingS3Storage", bound=S3Storage)


class FileStreamingResponse(FileResponse):
    """FileResponse FastAPI for streaming from S3."""

    chunk_size = 4096

    def __init__(self, storage: TypingS3Storage, **kwargs: typing.Any) -> None:
        self.storage = storage
        super().__init__(**kwargs)

    def set_stat_headers(
        self, stat_result: os.stat_result | None = None, s3_output: GetObjectOutputTypeDef | None = None
    ) -> None:
        if stat_result is not None:
            super().set_stat_headers(stat_result)
        elif s3_output is not None:
            s3_headers = s3_output["ResponseMetadata"]["HTTPHeaders"].copy()
            s3_headers.pop("content-disposition", None)
            self.headers.update(s3_headers)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            async with self.storage.get_object(self.path) as resp:
                self.set_stat_headers(s3_output=resp)
                content_length = resp["ContentLength"]
                sent_so_far = 0

                await send({"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers})

                if scope["method"].upper() == "HEAD":
                    await send({"type": "http.response.body", "body": b"", "more_body": False})
                else:
                    async for chunk in resp["Body"].iter_chunks(self.chunk_size):
                        sent_so_far += len(chunk)
                        more_body = sent_so_far != content_length
                        chunk_data = {"type": "http.response.body", "body": chunk, "more_body": more_body}
                        await send(chunk_data)

        except ObjectDoesNotExistError as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error

        if self.background is not None:
            await self.background()
