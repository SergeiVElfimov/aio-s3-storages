import typing
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()


class S3Settings(BaseSettings):
    """Settings for working with s3."""

    access_key_id: str = ""
    secret_access_key: str = ""
    bucket_name: str = ""
    use_ssl: bool = True
    verify: bool = True
    endpoint_url: str = ""
    connect_timeout: int = 5
    read_timeout: int = 5
    addressing_style: typing.Literal["auto", "virtual", "path"] = "auto"
    signature_version: str | None = None
    proxies: typing.Mapping[str, str] | None = None
    time_zone_name: str = "Europe/Moscow"

    @property
    def time_zone(self):
        return ZoneInfo(self.time_zone_name)

    model_config = SettingsConfigDict(env_prefix="AWS_", extra="ignore")


settings = S3Settings()
