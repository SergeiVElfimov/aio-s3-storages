from __future__ import annotations

import asyncio
import typing

import pytest

if typing.TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_s3.type_defs import PaginatorConfigTypeDef, ResponseMetadataTypeDef

    OpListMultUplName: typing.TypeAlias = typing.Literal["list_multipart_uploads"]


def assert_status_code(resp_meta: ResponseMetadataTypeDef, status_code: int) -> None:
    assert resp_meta["HTTPStatusCode"] == status_code


async def assert_num_uploads_found(
    s3_client: S3Client,
    bucket_name: str,
    operation: OpListMultUplName,
    num_uploads: int,
    *,
    max_items: int | None = None,
    num_attempts: int = 5,
) -> None:
    paginator = s3_client.get_paginator(operation)
    pagination_config: PaginatorConfigTypeDef = {}
    if max_items is not None:
        pagination_config["MaxItems"] = max_items
    for _ in range(num_attempts):
        pages = paginator.paginate(Bucket=bucket_name, PaginationConfig=pagination_config)
        responses = []
        async for page in pages:
            responses.append(page)
        # Sometimes it takes a while for all downloads to show up, especially
        # if the download was just created. If we don't see the expected number,
        # we retry up to `num_attempts` times before failing the check.
        amount_seen = len(responses[0]["Uploads"])
        if amount_seen == num_uploads:
            # Test passed.
            return
        else:
            # Let's get some sleep and try again.
            await asyncio.sleep(2)

        pytest.fail(f"Expected to see {num_uploads} uploads but saw: {amount_seen}")
