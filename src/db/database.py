"""Define session."""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Create an engine
engine = create_engine(os.environ["POSTGRES_ENGINE"])

# Create a configured "Session" class
Session = sessionmaker(bind=engine)

# Create a session instance
session = Session()
