from __future__ import annotations

import random
import string
import typing
from itertools import chain

from .asserts import assert_status_code

if typing.TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_s3.type_defs import DeleteObjectRequestRequestTypeDef


def random_bucketname() -> str:
    """Random bucket name.

    63 - maximum length of the bucket name.
    """
    return random_name()


def random_name() -> str:
    """Random string with presumably unique contents.

    The string contains only characters allowed for S3 buckets
    (letters, numbers, dot and hyphen).
    """
    return "".join(random.sample(string.ascii_lowercase, k=26))


async def recursive_delete(s3_client: S3Client, bucket_name: str) -> None:
    """Recursively deletes buckets with their contents.

    :param s3_client: S3 client.
    :param bucket_name: Bucket name.
    """
    paginator = s3_client.get_paginator("list_object_versions")
    async for n in paginator.paginate(Bucket=bucket_name):
        for obj in chain(n.get("Versions", []), n.get("DeleteMarkers", [])):
            kwargs: DeleteObjectRequestRequestTypeDef = {"Bucket": bucket_name, "Key": str(obj["Key"])}
            if "VersionId" in obj:
                kwargs["VersionId"] = str(obj["VersionId"])
            delete_result = await s3_client.delete_object(**kwargs)
            assert_status_code(delete_result["ResponseMetadata"], 204)

    empty_resp = await s3_client.delete_bucket(Bucket=bucket_name)
    assert_status_code(empty_resp["ResponseMetadata"], 204)
