from common.base import session
from common.tables import PprRawAll, PprCleanAll
from sqlalchemy import cast, Integer, Date


def insert_transactions(session):
    """
    Perform an insert operation to add new data to the clean table.

    This function retrieves transaction IDs from the clean table and fetches
    transactions from the raw table that do not already exist in the clean table.
    The selected columns are date_of_sale, address, postal_code, county, price,
    and description. The retrieved data is then inserted in bulk into the clean table.
    """
    with session.begin():
    
        # Retrieve all the transaction ids from the clean table
        print("[Load] Retrieving all the transaction ids from the clean table")
        clean_transaction_ids = session.query(PprCleanAll.transaction_id)

        print("[Load] Retrieving the transactions to insert")
        # date_of_sale and price needs to be casted as their
        # datatype is not string but, respectively, Date and Integer
        transactions_to_insert = session.query(
            cast(PprRawAll.date_of_sale, Date),
            PprRawAll.address,
            PprRawAll.postal_code,
            PprRawAll.county,
            cast(PprRawAll.price, Integer),
            PprRawAll.description,
        ).filter(~PprRawAll.transaction_id.in_(clean_transaction_ids))

        # Collect the data to insert in a list of dictionaries
        rows_to_insert = []
        for transaction in transactions_to_insert:
            row = {
                "date_of_sale": transaction[0],
                "address": transaction[1],
                "postal_code": transaction[2],
                "county": transaction[3],
                "price": transaction[4],
                "description": transaction[5],
            }
            rows_to_insert.append(row)

        print(f"[Load] Transactions to insert: {len(rows_to_insert)}")

        # Insert the rows in bulk
        if rows_to_insert:
            print("[Load] Inserting")
            session.bulk_insert_mappings(PprCleanAll, rows_to_insert)

        # Execute and commit the statement to make changes in the database.
        session.commit()


def delete_transactions(session):
    """
    Perform a delete operation to remove rows not present in the last snapshot from the clean table.

    This function identifies transactions in the clean table that do not have a corresponding entry
    in the raw table (ppr_raw_all) and deletes those transactions from the clean table.

    The delete operation is essential to ensure that the clean table accurately reflects the most
    recent data available. By removing transactions that are no longer present in the raw table,
    the clean table stays synchronized with the latest snapshot, preventing outdated or irrelevant
    data from persisting.
    """
    with session.begin():
        # Get all ppr_raw_all transaction ids
        raw_transaction_ids = session.query(PprRawAll.transaction_id)

        # Filter all the ppt_clean_all table transactions that are not present in the ppr_raw_all table
        # and delete them.
        # Passing synchronize_session as argument for the delete method.
        transactions_to_delete = session.query(PprCleanAll).filter(
            ~PprCleanAll.transaction_id.in_(raw_transaction_ids)
        )

        # Print transactions to delete
        print("[Load] Transactions to delete:", transactions_to_delete.count())

        # Delete transactions
        transactions_to_delete.delete(synchronize_session=False)

        # Commit the session to make the changes in the database
        session.commit()


def main():
    print("[Load] Start")
    insert_transactions(session)
    delete_transactions(session)
    print("[Load] End")
    print("Ending session...")
    session.close()
    print("Session ended.")
