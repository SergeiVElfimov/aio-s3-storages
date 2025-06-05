# fastapi-s3-storages

S3 backend storage to simplify file management in FastAPI.
Similar to django-storages project.

## Examples

```python
from partners_utils.storage import S3Storage
class TestS3Storage(S3Storage):
    bucket_name = "Bucket name"
    location = "Folder name"

from partners_utils.storage import FileStreamingResponse
@router.get(
    "/test-file/{attachment_id}/",
    response_class=FileStreamingResponse,
    summary="Get file",
)
async def get_file(
    attachment_id: UUID,
    attachment_service: TestService = Depends(),
):
    attachment = await attachment_service.get_attachment(attachment_id=attachment_id)
    return FileStreamingResponse(storage=attachment_service.storage, filename=attachment.name, path=attachment.file)
```

## Environment variables

AWS_ACCESS_KEY_ID - Access key.
AWS_SECRET_ACCESS_KEY - Secret key.
AWS_USE_SSL - Use SSL encryption.
AWS_VERIFY - SSL certificate verification.
AWS_ENDPOINT_URL - Host for working with S3.
AWS_CONNECT_TIMEOUT - Maximum connection establishment time.
AWS_READ_TIMEOUT - Maximum data retrieval time.
AWS_ADDRESSING_STYLE - Addressing style.
AWS_SIGNATURE_VERSION - Signature version.
AWS_PROXIES - Proxy.
AWS_TIME_ZONE_NAME - setting time zone (default value "Europe/Moscow").

## Required
- python >=3.11, <4.0
- fastapi >=0.100.0, <1.0
- SQLAlchemy >=1.4.36, <2.1.0
- pydantic >=2.0.0, <3.0.0
- pydantic-settings >=2.0.0
- python-dotenv >=1.0.0

## Installation
```pip install django-excel-viewset```

## Contribution

You can run tests with `pytest`.

```
pip install poetry
poetry install
pytest
```
