from __future__ import annotations

import os
import typing
from contextlib import ExitStack

import pytest
from aiobotocore.config import AioConfig
from aiobotocore.session import AioSession

from .asserts import assert_num_uploads_found, assert_status_code
from .helpers import random_bucketname, recursive_delete
from .moto_server import MotoService

if typing.TYPE_CHECKING:
    from asyncio.events import AbstractEventLoop
    from collections import abc

    from _pytest.fixtures import SubRequest
    from botocore.config import Config
    from pytest import FixtureRequest
    from pytest_mock import MockerFixture
    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_s3.type_defs import PutObjectOutputTypeDef

    AsyncObjectMaker: typing.TypeAlias = abc.Callable[..., abc.Awaitable[PutObjectOutputTypeDef]]
    AsyncBucketMaker: typing.TypeAlias = abc.Callable[[str], abc.Awaitable[str]]


host = "127.0.0.1"

_PYCHARM_HOSTED = os.environ.get("PYCHARM_HOSTED") == "1"


@pytest.fixture(scope="session", params=[True, False], ids=["debug[true]", "debug[false]"])
def debug(request: SubRequest) -> bool:
    return request.param


@pytest.fixture
async def s3_client(
    session: AioSession,
    region: str,
    config: Config,
    s3_server: str,
    mocking_test: bool,
    s3_verify: typing.Any,
    patch_attributes: typing.Any,
) -> typing.AsyncGenerator[S3Client, None]:
    # This depends on mock_attributes, as we may need to test event listeners.
    # See the `mock_attributes` documentation for details.
    kw = moto_config(s3_server) if mocking_test else {}
    kw.update(region_name=region, config=config, verify=s3_verify)
    async with session.create_client("s3", **kw) as client:
        yield client


@pytest.fixture
def session() -> AioSession:
    return AioSession()


@pytest.fixture
def config(request: FixtureRequest, region: str, signature_version: str) -> AioConfig:
    config_kwargs = request.node.get_closest_marker("config_kwargs") or {}
    if config_kwargs:
        assert not hasattr(config_kwargs, "kwargs"), config_kwargs
        assert len(config_kwargs.args) == 1
        config_kwargs = config_kwargs.args[0]

    connect_timeout = read_timout = 5
    if _PYCHARM_HOSTED:
        connect_timeout = read_timout = 180

    config_kwargs.update(
        region_name=region,
        signature_version=signature_version,
        read_timeout=read_timout,
        connect_timeout=connect_timeout,
    )
    return AioConfig(**config_kwargs)


@pytest.fixture
def region() -> str:
    return "us-east-1"


@pytest.fixture
async def s3_server(server_scheme: str) -> abc.AsyncGenerator[str, None]:
    async with MotoService("s3", ssl=server_scheme == "https") as svc:
        yield svc.endpoint_url


@pytest.fixture
def server_scheme() -> str:
    return "http"


@pytest.fixture
def signature_version() -> str:
    return "s3"


@pytest.fixture
def mocking_test() -> bool:
    """Flag for tests with real AWS."""
    return True


@pytest.fixture
def s3_verify() -> typing.Any:
    return None


@pytest.fixture
def patch_attributes(request: FixtureRequest, mocker: MockerFixture) -> typing.Any:
    """unittest.mock.patch on the arguments passed via the pytest mark (`pytest.mark`).

    This fixture looks at the @pytest.mark.patch_attributes mark. This mark is a list of
    arguments to pass to unittest.mock.patch (see example below). This fixture
    returns a list of mock objects, one per element in the input list.

    Why do we need this? In some cases, we want to patch before other fixtures are run.

    For example, the `s3_client` fixture creates an aiobotocore client. During the process of creating the client,

    some event listeners are registered. When we want to patch the target of these event listeners,

    we must do so before the `s3_client` fixture is run. Otherwise, the aiobotocore client will retain

    references to the unpatched targets.

    In such situations, make sure that subsequent fixtures explicitly depend on `patch_attribute` to
    ensure order between fixtures.

    Example:
    @pytest.mark.patch_attributes([
        dict(
            target="aiobotocore.retries.adaptive.AsyncClientRateLimiter.on_sending_request",
            side_effect=aiobotocore.retries.adaptive.AsyncClientRateLimiter.on_sending_request,
            autospec=True
        )
    ])
    async def test_client_rate_limiter_called(s3_client, patch_attributes):
        await s3_client.get_object(Bucket="bucket", Key="key")
        # Just for illustration (this test doesn't pass).
        # mock_attributes is a list of 1 element, since we passed a list of 1 element
        # to the patch_attributes marker.
        mock_attributes[0].assert_called_once()
    """
    marker = request.node.get_closest_marker("patch_attributes")
    if marker is None:
        yield
    else:
        with ExitStack() as stack:
            yield [stack.enter_context(mocker.patch(**kwargs)) for kwargs in marker.args[0]]


def moto_config(endpoint_url: str) -> dict[str, typing.Any]:
    return {"endpoint_url": endpoint_url, "aws_secret_access_key": "xxx", "aws_access_key_id": "xxx"}


@pytest.fixture
def create_object(s3_client: S3Client, bucket_name: str) -> AsyncObjectMaker:
    async def _f(key_name: str, body: str | bytes = "foo") -> PutObjectOutputTypeDef:
        put_result = await s3_client.put_object(Bucket=bucket_name, Key=key_name, Body=body)
        assert_status_code(put_result["ResponseMetadata"], 200)
        return put_result

    return _f


@pytest.fixture
def create_multipart_upload(
    request: FixtureRequest, s3_client: S3Client, bucket_name: str, event_loop: AbstractEventLoop
) -> abc.Callable[[str], abc.Awaitable[str]]:
    _key_name, upload_id = "", ""

    async def _f(key_name: str) -> str:
        nonlocal _key_name
        nonlocal upload_id
        _key_name = key_name

        parsed = await s3_client.create_multipart_upload(Bucket=bucket_name, Key=key_name)
        upload_id = parsed["UploadId"]
        return upload_id

    def fin() -> None:
        event_loop.run_until_complete(
            s3_client.abort_multipart_upload(UploadId=upload_id, Bucket=bucket_name, Key=_key_name)
        )

    request.addfinalizer(fin)
    return _f


@pytest.fixture
async def bucket_name(region: str, create_bucket: AsyncBucketMaker) -> abc.AsyncGenerator[str, None]:
    yield await create_bucket(region)


@pytest.fixture
async def create_bucket(s3_client: S3Client) -> abc.AsyncGenerator[AsyncBucketMaker, None]:
    _bucket_name = ""

    async def _f(region_name: str, bucket_name: str | None = None) -> str:
        nonlocal _bucket_name
        if bucket_name is None:
            bucket_name = random_bucketname()
        _bucket_name = bucket_name
        bucket_kwargs: dict[str, typing.Any] = {"Bucket": bucket_name}
        if region_name != "us-east-1":
            bucket_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region_name}
        create_result = await s3_client.create_bucket(**bucket_kwargs)
        assert_status_code(create_result["ResponseMetadata"], 200)
        await s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"})
        return bucket_name

    try:
        yield _f
    finally:
        await recursive_delete(s3_client, _bucket_name)


def pytest_configure() -> None:
    class AIOUtils:
        def __init__(self) -> None:
            self.assert_status_code = assert_status_code
            self.assert_num_uploads_found = assert_num_uploads_found

    pytest.aio = AIOUtils()
