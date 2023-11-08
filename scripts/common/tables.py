from sqlalchemy import cast, Column, Integer, String, Date
# Import the function required
from sqlalchemy.orm import column_property

from .base import Base

class PprRawAll(Base):
    __tablename__ = "ppr_raw_all"

    id = Column(Integer, primary_key=True)  # Primary key column for unique identification
    date_of_sale = Column(String(255))  # Column for date of sale
    address = Column(String(255))  # Column for property address
    postal_code = Column(String(255))  # Column for postal code
    county = Column(String(255))  # Column for county
    price = Column(String(255))  # Column for property price
    description = Column(String(255))  # Column for property description
    # Create a unique transaction id
    transaction_id = column_property(
        date_of_sale + "_" + address + "_" + county + "_" + price
    )

class PprCleanAll(Base):
    __tablename__ = "ppr_clean_all"

    id = Column(Integer, primary_key=True)  # Primary key column for unique identification
    date_of_sale = Column(Date)  # Column for date of sale
    address = Column(String(255))  # Column for property address
    postal_code = Column(String(255))  # Column for postal code
    county = Column(String(255))  # Column for county
    price = Column(Integer)  # Column for property price
    description = Column(String(255))  # Column for property description
    # Create a unique transaction id
    # all non-string columns are casted as string
    transaction_id = column_property(
        cast(date_of_sale, String) + "_" + address + "_" + county + "_" + cast(price, String)
    )