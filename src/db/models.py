"""Models for creating tables."""

from sqlalchemy import Column, Integer, VARCHAR
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base table for declarations."""
    pass


class Client(Base):
    """Table for mapping clients from Sales to Portal."""
    __tablename__ = 'clients'

    sales_id = Column(Integer, primary_key=True)
    portal_id = Column(VARCHAR)
