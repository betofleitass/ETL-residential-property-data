# Import the necessary components from the 'base' and 'tables' modules
from base import Base, engine
from tables import PprRawAll

# Loop through the tables defined in the metadata of the 'Base' class and print their names
for table in Base.metadata.tables:
    print(table)

# Check if this script is the main module being run
if __name__ == "__main__":
    # If it is the main module, create all the database tables defined in the 'Base' class
    Base.metadata.create_all(engine)
