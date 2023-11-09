# Import the necessary components from the 'base' and 'tables' modules
from .base import Base, engine
from .tables import PprRawAll, PprCleanAll


# Loop through the tables defined in the metadata of the 'Base' class and print their names
for table in Base.metadata.tables:
    print(table)

# Check if this script is the main module being run
if __name__ == "__main__":
    # If it is the main module, create all the database tables defined in the 'Base' class
    try:
        Base.metadata.create_all(engine)
        print("Tables created successfully.")
    except Exception as e:
        print(f"Error creating tables: {str(e)}")