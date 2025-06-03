"""Models for creating tables."""

from sqlalchemy import Column, Integer, VARCHAR
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base table for declarations."""
    pass


class Client(Base):
    """Table for mapping clients from Sales to Portal."""
    __tablename__ = 'clients'

    portal_id = Column(Integer, primary_key=True)
    sales_id = Column(VARCHAR, unique=True)
