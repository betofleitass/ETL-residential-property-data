import extract
import transform
import load

# Entry point for the ETL (Extract, Transform, Load) process.
if __name__ == "__main__":
    """
    Main script for the ETL process.

    This script orchestrates the entire ETL (Extract, Transform, Load) workflow by
    sequentially executing the main functions of each phase.

    - Extract: The 'extract.main()' function downloads the latest data snapshot,
      extracts relevant information, and saves it in a raw format.

    - Transform: The 'transform.main()' function processes the raw data, applies
      transformations, and prepares it for loading into the database.

    - Load: The 'load.main()' function loads the transformed data into the database,
      updating existing records and inserting new ones.
    """
    extract.main()
    transform.main()
    load.main()
