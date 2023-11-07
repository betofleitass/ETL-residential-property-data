# Import necessary components from the 'base' module and SQLAlchemy
from .base import Base
from sqlalchemy import Column, Integer, String

# Define an SQLAlchemy ORM (Object-Relational Mapping) class for the 'ppr_raw_all' table
class PprRawAll(Base):
    # Set the table name for this class
    __tablename__ = "ppr_raw_all"
    
    # Define columns for the 'ppr_raw_all' table
    id = Column(Integer, primary_key=True)  # Primary key column for unique identification
    date_of_sale = Column(String(255))  # Column for date of sale
    address = Column(String(255))  # Column for property address
    postal_code = Column(String(255))  # Column for postal code
    county = Column(String(255))  # Column for county
    eircode = Column(String(255))  # Column for Eircode
    price = Column(String(255))  # Column for property price
    not_full_market_price = Column(String(255))  # Column for indicating if the price is not the full market price
    vat_exclusive = Column(String(255))  # Column for indicating if the price is VAT exclusive
    description = Column(String(255))  # Column for property description
    property_size_description = Column(String(255))  # Column for property size description
