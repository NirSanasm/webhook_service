"""
SQLAlchemy declarative base.
All models import from here to share the same metadata registry.
"""
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass

