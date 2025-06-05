import mimetypes
import pathlib
from datetime import datetime
from functools import cached_property
from uuid import uuid4

from sqlalchemy import TIMESTAMP, UUID, String
from sqlalchemy.orm import Mapped, declarative_mixin, mapped_column
from sqlalchemy.sql import func

from .constants import DEFUALT_MIME_TYPE, MAPPING_MIME_TYPE_AND_FORMAT_FILE
from .settings import settings


@declarative_mixin
class S3AttachmentMixin:
    """Mixin for storing meta information about a file."""

    id: Mapped[str] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4, comment="UUID.")
    name: Mapped[str] = mapped_column(String(length=500), comment="Filename.")
    file: Mapped[str] = mapped_column(String(length=510), comment="Path to file in S3.")
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        default=func.now(tz=settings.time_zone),
        comment="Datetime of creation.",
    )

    @cached_property
    def mime_type(self) -> str:
        mime, _ = mimetypes.guess_type(self.name)
        if not mime:
            mime = MAPPING_MIME_TYPE_AND_FORMAT_FILE.get(pathlib.Path(self.name).suffix.lower(), DEFUALT_MIME_TYPE)
        return mime

    @cached_property
    def original_file(self) -> str:
        """Returning a link to a file."""
        raise NotImplementedError
