"""SQLAlchemy class representing DBResource row. Holds dynamic resource metadata for
each run.
"""
from datetime import datetime

from hdx.database.no_timezone import Base
from sqlalchemy.orm import Mapped, mapped_column


class DBResource(Base):
    """
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(nullable=False)
    dataset_ref: Mapped[int] = mapped_column(
        ForeignKey("dbdatasets.id"), nullable=False
    )
    hdx_link: Mapped[str] = mapped_column(unique=True, nullable=False)
    filename: Mapped[str] = mapped_column(nullable=False)
    mime_type: Mapped[str] = mapped_column(nullable=False)
    last_modified: Mapped[datetime] = mapped_column(nullable=False)
    is_hxl: Mapped[bool] = mapped_column(nullable=False)
    api_link: Mapped[str] = mapped_column(unique=True, nullable=False)
    """

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(nullable=False)
    dataset_ref: Mapped[int] = mapped_column(
        # ForeignKey("dbdatasets.id"),
        nullable=False
    )
    hdx_link: Mapped[str] = mapped_column(unique=True, nullable=False)
    filename: Mapped[str] = mapped_column(nullable=False)
    mime_type: Mapped[str] = mapped_column(nullable=False)
    last_modified: Mapped[datetime] = mapped_column(nullable=False)
    is_hxl: Mapped[bool] = mapped_column(nullable=False)
    api_link: Mapped[str] = mapped_column(unique=True, nullable=False)

    def __repr__(self) -> str:
        """String representation of DBResource row

        Returns:
            str: String representation of DBResource row
        """
        # Can add more fields to this if useful
        output = f"<Resource(id={self.id}, code={self.code}, "
        output += (
            f"dataset ref={self.dataset_ref},\nhdx_link={self.hdx_link},\n"
        )
        output += f"last modified={str(self.last_modified)})>"
        return output
