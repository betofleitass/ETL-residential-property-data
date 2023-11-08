from common.base import session
from common.tables import PprRawAll, PprCleanAll

from sqlalchemy import cast, Integer, Date
from sqlalchemy.dialects.postgresql import insert


def insert_transactions():
    """
    Insert operation: add new data
    """
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



def delete_transactions():
    """
    Delete operation: delete any row not present in the last snapshot
    """
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
    insert_transactions()
    delete_transactions()
    print("[Load] End")
    print("Ending session...")
    session.close()
