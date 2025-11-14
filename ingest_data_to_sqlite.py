"""
Ingest generated e-commerce CSV datasets into a SQLite database.

The script reads the CSV exports created by `generate_ecommerce_data.py`, creates
the appropriate tables (with constraints and indexes), and loads the data using
transactions to maintain integrity. It prints progress updates and ingestion
statistics for transparency during execution.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd

DATA_DIR = Path("data")
DATABASE_PATH = Path("ecommerce.db")


TABLE_QUERIES: Dict[str, str] = {
    "customers": """
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            country TEXT,
            registration_date DATE
        );
    """,
    "products": """
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            description TEXT,
            price REAL NOT NULL,
            stock_quantity INTEGER,
            supplier TEXT,
            created_date DATE
        );
    """,
    "orders": """
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date DATE NOT NULL,
            total_amount REAL,
            status TEXT,
            payment_method TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
    """,
    "order_items": """
        CREATE TABLE order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            subtotal REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );
    """,
    "reviews": """
        CREATE TABLE reviews (
            review_id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            rating INTEGER CHECK(rating BETWEEN 1 AND 5),
            review_text TEXT,
            review_date DATE,
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
    """,
}

INDEX_QUERIES: Dict[str, str] = {
    "idx_orders_customer": "CREATE INDEX idx_orders_customer ON orders(customer_id);",
    "idx_order_items_order": "CREATE INDEX idx_order_items_order ON order_items(order_id);",
    "idx_order_items_product": "CREATE INDEX idx_order_items_product ON order_items(product_id);",
    "idx_reviews_product": "CREATE INDEX idx_reviews_product ON reviews(product_id);",
    "idx_reviews_customer": "CREATE INDEX idx_reviews_customer ON reviews(customer_id);",
}


def read_csv(filename: str) -> pd.DataFrame:
    """
    Load a CSV file into a DataFrame with basic validation.

    Parameters
    ----------
    filename:
        Name of the CSV file located under DATA_DIR.
    """
    path = DATA_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Required file missing: {path}")
    return pd.read_csv(path)


def load_dataframes() -> Dict[str, pd.DataFrame]:
    """Read all required CSV files and return them keyed by table name."""
    print("Reading CSV files...")
    dataframes = {
        "customers": read_csv("customers.csv"),
        "products": read_csv("products.csv"),
        "orders": read_csv("orders.csv"),
        "order_items": read_csv("order_items.csv"),
        "reviews": read_csv("reviews.csv"),
    }
    for name, df in dataframes.items():
        print(f"  Loaded {name} with {len(df)} rows.")
    return dataframes


def connect_database(path: Path) -> sqlite3.Connection:
    """Create a SQLite connection with foreign key support enabled."""
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection


def drop_existing_tables(connection: sqlite3.Connection) -> None:
    """Drop tables in reverse dependency order to allow re-runs."""
    table_order = ["reviews", "order_items", "orders", "products", "customers"]
    cursor = connection.cursor()
    for table in table_order:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
    connection.commit()


def create_schema(connection: sqlite3.Connection) -> None:
    """Create tables and indexes according to the defined schema."""
    cursor = connection.cursor()
    for name, query in TABLE_QUERIES.items():
        print(f"Creating table {name}...")
        cursor.execute(query)
    for index_name, index_query in INDEX_QUERIES.items():
        print(f"Creating index {index_name}...")
        cursor.execute(index_query)
    connection.commit()


def iter_records(df: pd.DataFrame) -> Iterable[Tuple]:
    """
    Yield tuples matching DataFrame rows for insertion.

    Using `itertuples(index=False, name=None)` preserves column order and avoids
    the overhead of creating pandas namedtuples.
    """
    return df.itertuples(index=False, name=None)


def insert_data(connection: sqlite3.Connection, dataframes: Dict[str, pd.DataFrame]) -> None:
    """
    Insert DataFrame contents into corresponding tables using transactions.

    Parameters
    ----------
    connection:
        Open SQLite connection with autocommit disabled.
    dataframes:
        Mapping of table names to populated DataFrames.
    """
    insert_statements = {
        "customers": """
            INSERT INTO customers (
                customer_id, first_name, last_name, email, phone, address, city,
                state, zip_code, country, registration_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        "products": """
            INSERT INTO products (
                product_id, product_name, category, description, price,
                stock_quantity, supplier, created_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        "orders": """
            INSERT INTO orders (
                order_id, customer_id, order_date, total_amount, status, payment_method
            ) VALUES (?, ?, ?, ?, ?, ?);
        """,
        "order_items": """
            INSERT INTO order_items (
                order_item_id, order_id, product_id, quantity, unit_price, subtotal
            ) VALUES (?, ?, ?, ?, ?, ?);
        """,
        "reviews": """
            INSERT INTO reviews (
                review_id, product_id, customer_id, rating, review_text, review_date
            ) VALUES (?, ?, ?, ?, ?, ?);
        """,
    }

    cursor = connection.cursor()
    try:
        cursor.execute("BEGIN;")
        for table_name, statement in insert_statements.items():
            df = dataframes[table_name]
            rows = list(iter_records(df))
            print(f"Inserting {len(rows)} rows into {table_name}...")
            cursor.executemany(statement, rows)
        connection.commit()
    except sqlite3.DatabaseError as error:
        connection.rollback()
        raise RuntimeError(f"Insertion failed: {error}") from error


def report_counts(connection: sqlite3.Connection) -> None:
    """Print row counts for all tables."""
    cursor = connection.cursor()
    for table in ["customers", "products", "orders", "order_items", "reviews"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        count = cursor.fetchone()[0]
        print(f"Table {table} contains {count} rows.")


def main() -> None:
    """Orchestrate ingestion from CSV files into SQLite."""
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"Data directory not found: {DATA_DIR.resolve()}")

    dataframes = load_dataframes()
    with connect_database(DATABASE_PATH) as connection:
        drop_existing_tables(connection)
        create_schema(connection)
        insert_data(connection, dataframes)
        report_counts(connection)
    print(f"Ingestion completed. Database stored at {DATABASE_PATH.resolve()}")


if __name__ == "__main__":
    main()

