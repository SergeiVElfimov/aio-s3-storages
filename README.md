# aio-s3-storages

[![build-status-image]][build-status]
[![pypi-version]][pypi]
[![codeql-image]][codeql]

S3 backend storage to simplify file management.
Similar to django-storages project.

## Examples

```python
from partners_utils.storage import S3Storage


class TestS3Storage(S3Storage):
    bucket_name = "Bucket name"
    location = "Folder name"

storage = TestS3Storage()
# get file by name
await self.storage.open(name="path_to_file")
# save file to s3 from local storage
with open("path_to_file_into_local_storage", "rb") as _file:
    extra_args = {"ContentType": "content_type_file"}
    await self.storage.save(name="path_to_file", content=_file, extra_args=extra_args)
# delete file
await self.storage.delete(name="path_to_file")
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

AWS_TIME_ZONE_NAME - Setting time zone (default value "Europe/Moscow").

## Required

- python >=3.11, <4.0
- aioboto3 >=12.4.0, <14.0
- pydantic >=2.0.0, <3.0.0
- pydantic-settings >=2.0.0
- python-dotenv >=1.0.0
- tzdata = >=2024.1

## Installation

```pip install aio-s3-storages```

## Contributing

Before contributing please read our [contributing guidelines](CONTRIBUTING.md).

## Acknowledgments

I express my deep gratitude for the help in working on the project [Rinat Akhtamov](https://github.com/rinaatt) and [Albert Alexandrov](https://github.com/albertalexandrov)

[build-status-image]: https://github.com/SergeiVElfimov/aio-s3-storages/actions/workflows/python-package.yml/badge.svg
[build-status]: https://github.com/SergeiVElfimov/aio-s3-storages/actions/workflows/python-package.yml
[pypi-version]: https://img.shields.io/pypi/v/aio-s3-storages.svg
[pypi]: https://pypi.org/project/aio-s3-storages/
[codeql-image]: https://github.com/SergeiVElfimov/aio-s3-storages/actions/workflows/codeql.yml/badge.svg
[codeql]: https://github.com/SergeiVElfimov/aio-s3-storages/actions/workflows/codeql.yml
