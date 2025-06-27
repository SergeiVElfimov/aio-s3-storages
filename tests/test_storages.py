FILE_NAME = "test file.abc"


class TestStorage:
    file_key = "some-dir/some-file.abc"
    content = b"<file content>" * 10

    async def test_download_file(self, create_object, s3_storage_client_mock) -> None:
        await create_object(self.file_key, body=self.content)
        storage = s3_storage_client_mock()
        _file = await storage.open(name=self.file_key)
        assert _file.getvalue() == self.content
