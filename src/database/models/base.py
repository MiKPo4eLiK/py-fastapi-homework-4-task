from typing import Optional
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    @classmethod
    def default_order_by(cls) -> Optional[ClauseElement]:
        """Return a SQLAlchemy expression for default ordering or None."""
        return None
