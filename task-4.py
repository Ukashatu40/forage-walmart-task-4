import sqlite3
import pandas as pd

def create_product_table(cursor):
    """Creates the 'product' table if it doesn't exist."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS product (
            id INTEGER NOT NULL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
    ''')

def create_shipment_table(cursor):
    """Creates the 'shipment' table if it doesn't exist."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS shipment (
            id INTEGER NOT NULL PRIMARY KEY,
            product_id INTEGER NOT NULL REFERENCES product,
            quantity INTEGER NOT NULL,
            origin TEXT NOT NULL,
            destination TEXT NOT NULL
        )
    ''')

def populate_database(conn):
    """
    Reads data from CSV files and populates the database tables.
    """
    cur = conn.cursor()

    try:
        # Read the CSV files into pandas DataFrames
        df0 = pd.read_csv('data/shipping_data_0.csv')
        df1 = pd.read_csv('data/shipping_data_1.csv')
        df2 = pd.read_csv('data/shipping_data_2.csv')

        # 1. Populate the 'product' table with unique product names
        # Combine product names from both datasets
        unique_products = pd.concat([df0['product'], df1['product']]).unique()

        # Insert unique products into the 'product' table
        for product_name in unique_products:
            cur.execute('''
                INSERT OR IGNORE INTO product (name) VALUES (?)
            ''', (product_name,))
        conn.commit()

        # Get a dictionary of product names and their IDs for quick lookup
        product_ids = {name: id for id, name in cur.execute('SELECT id, name FROM product').fetchall()}

        # 2. Populate the 'shipment' table from shipping_data_0.csv
        # This file is self-contained and each row represents a unique shipment.
        for index, row in df0.iterrows():
            product_id = product_ids.get(row['product'])
            if product_id is not None:
                cur.execute('''
                    INSERT INTO shipment (product_id, quantity, origin, destination)
                    VALUES (?, ?, ?, ?)
                ''', (product_id, row['product_quantity'], row['origin_warehouse'], row['destination_store']))

        # 3. Populate the 'shipment' table from shipping_data_1.csv and shipping_data_2.csv
        # Merge the two dataframes on the common 'shipment_identifier'
        merged_df = pd.merge(df1, df2, on='shipment_identifier')

        # Insert data from the merged dataframe. The prompt states that
        # spreadsheet 1 contains a single product per row, so quantity is 1.
        for index, row in merged_df.iterrows():
            product_id = product_ids.get(row['product'])
            if product_id is not None:
                cur.execute('''
                    INSERT INTO shipment (product_id, quantity, origin, destination)
                    VALUES (?, ?, ?, ?)
                ''', (product_id, 1, row['origin_warehouse'], row['destination_store']))

        # Commit all changes to the database
        conn.commit()
        print("Database populated successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()  # Roll back any changes if an error occurred

def main():
    """Main function to run the script."""
    # Connect to the SQLite database
    conn = sqlite3.connect('shipment_database.db')
    cur = conn.cursor()

    # The schema is already provided, so we don't need to create the tables.
    # If the tables did not exist, you would call:
    # create_product_table(cur)
    # create_shipment_table(cur)

    # Populate the database with data from the CSV files
    populate_database(conn)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()