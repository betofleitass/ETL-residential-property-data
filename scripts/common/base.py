# Import necessary modules from SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, Session

# Define the connection URL for a PostgreSQL database
url = "postgresql+psycopg2://postgres:postgres@localhost:5432/residential_property_data_prod"

# Create a database engine using the URL
engine = create_engine(url)

# Create a session
session = Session(engine)

# Create a base class for declarative class definitions
Base = declarative_base()
