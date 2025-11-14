"""
Script to generate synthetic e-commerce CSV datasets with realistic relationships.

The script produces customers, products, orders, order_items, and reviews datasets,
ensuring referential integrity between entities. It relies on Faker for realistic
values and pandas for convenient CSV export.
"""
from __future__ import annotations

import random
from datetime import date
from pathlib import Path
from typing import Dict, List

import pandas as pd
from faker import Faker

DATA_DIR = Path("data")
CUSTOMER_COUNT = 1_000
PRODUCT_COUNT = 500
ORDER_COUNT = 2_000
ORDER_ITEM_COUNT = 5_000
REVIEW_COUNT = 1_500
REGISTRATION_START = date(2023, 1, 1)
DATA_END_DATE = date(2024, 11, 30)


def ensure_data_directory(directory: Path) -> None:
    """
    Create the target directory if it does not exist.

    Parameters
    ----------
    directory:
        Destination directory for generated CSV files.
    """
    directory.mkdir(parents=True, exist_ok=True)


def generate_customers(fake: Faker, count: int) -> pd.DataFrame:
    """
    Generate customer records.

    Parameters
    ----------
    fake:
        Faker instance configured for realistic values.
    count:
        Number of customers to create.
    """
    customers: List[Dict[str, object]] = []
    for customer_id in range(1, count + 1):
        registration_date = fake.date_between_dates(REGISTRATION_START, DATA_END_DATE)
        customers.append(
            {
                "customer_id": customer_id,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.unique.email(),
                "phone": fake.phone_number(),
                "address": fake.street_address(),
                "city": fake.city(),
                "state": fake.state(),
                "zip_code": fake.postcode(),
                "country": fake.country(),
                "registration_date": registration_date,
            }
        )
    return pd.DataFrame(customers)


def generate_products(fake: Faker, count: int) -> pd.DataFrame:
    """
    Generate product catalog entries.

    Parameters
    ----------
    fake:
        Faker instance configured for realistic values.
    count:
        Number of products to create.
    """
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books"]
    products: List[Dict[str, object]] = []
    for product_id in range(1, count + 1):
        price = round(random.uniform(5, 500), 2)
        products.append(
            {
                "product_id": product_id,
                "product_name": fake.catch_phrase(),
                "category": random.choice(categories),
                "description": fake.text(max_nb_chars=120),
                "price": price,
                "stock_quantity": random.randint(0, 1_000),
                "supplier": fake.company(),
                "created_date": fake.date_between_dates(REGISTRATION_START, DATA_END_DATE),
            }
        )
    return pd.DataFrame(products)


def generate_orders(fake: Faker, customers_df: pd.DataFrame, count: int) -> pd.DataFrame:
    """
    Generate orders tied to customers.

    Parameters
    ----------
    fake:
        Faker instance configured for realistic values.
    customers_df:
        DataFrame of customer records.
    count:
        Number of orders to create.
    """
    statuses = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]
    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Cash on Delivery"]
    customers = customers_df.to_dict("records")

    orders: List[Dict[str, object]] = []
    for order_id in range(1, count + 1):
        customer = random.choice(customers)
        order_date = fake.date_between_dates(customer["registration_date"], DATA_END_DATE)
        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "order_date": order_date,
                "total_amount": 0.0,  # updated after order items generation
                "status": random.choice(statuses),
                "payment_method": random.choice(payment_methods),
            }
        )
    return pd.DataFrame(orders)


def generate_order_items(
    fake: Faker,
    orders_df: pd.DataFrame,
    products_df: pd.DataFrame,
    count: int,
) -> pd.DataFrame:
    """
    Generate order items that link orders and products.

    The function ensures each order has at least one item before filling the
    remaining rows to satisfy the required count.
    """
    products = products_df.set_index("product_id")
    orders = orders_df.set_index("order_id")
    order_totals: Dict[int, float] = {order_id: 0.0 for order_id in orders.index}

    def build_item(record_id: int, order_id: int) -> Dict[str, object]:
        """Helper to create a single order item record."""
        product_id = random.choice(products_df["product_id"].tolist())
        price = float(products.loc[product_id, "price"])
        quantity = random.randint(1, 5)
        subtotal = round(quantity * price, 2)
        order_totals[order_id] += subtotal
        return {
            "order_item_id": record_id,
            "order_id": order_id,
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": price,
            "subtotal": subtotal,
        }

    order_items: List[Dict[str, object]] = []
    record_id = 1

    for order_id in orders.index:
        order_items.append(build_item(record_id, order_id))
        record_id += 1

    while record_id <= count:
        random_order_id = random.choice(orders_df["order_id"].tolist())
        order_items.append(build_item(record_id, random_order_id))
        record_id += 1

    orders_df["total_amount"] = orders_df["order_id"].map(order_totals).round(2)
    return pd.DataFrame(order_items)


def generate_reviews(
    fake: Faker,
    order_items_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    count: int,
) -> pd.DataFrame:
    """
    Generate product reviews linked to valid customer purchases.

    Parameters
    ----------
    fake:
        Faker instance configured for realistic values.
    order_items_df:
        DataFrame of order item records.
    orders_df:
        DataFrame of orders containing customer relationships.
    count:
        Number of reviews to create.
    """
    order_lookup = orders_df.set_index("order_id")
    selectable_items = order_items_df.sample(n=count, replace=True, random_state=42)

    reviews: List[Dict[str, object]] = []
    for review_id, (_, item) in enumerate(selectable_items.iterrows(), start=1):
        order_id = int(item["order_id"])
        order = order_lookup.loc[order_id]
        order_date = order["order_date"]
        review_date = fake.date_between_dates(order_date, DATA_END_DATE)
        reviews.append(
            {
                "review_id": review_id,
                "product_id": int(item["product_id"]),
                "customer_id": int(order["customer_id"]),
                "rating": random.randint(1, 5),
                "review_text": fake.paragraph(nb_sentences=3),
                "review_date": review_date,
            }
        )
    return pd.DataFrame(reviews)


def save_dataframe(df: pd.DataFrame, filename: str) -> None:
    """
    Persist a DataFrame to CSV under the data directory.

    Parameters
    ----------
    df:
        DataFrame to serialise.
    filename:
        Target filename within the data directory.
    """
    try:
        path = DATA_DIR / filename
        df.to_csv(path, index=False)
        print(f"Saved {filename} with {len(df)} records.")
    except OSError as error:
        raise RuntimeError(f"Unable to write {filename}: {error}") from error


def main() -> None:
    """Entry point for dataset generation."""
    random.seed(42)
    fake = Faker()
    Faker.seed(42)

    ensure_data_directory(DATA_DIR)

    try:
        print("Generating customers...")
        customers_df = generate_customers(fake, CUSTOMER_COUNT)

        print("Generating products...")
        products_df = generate_products(fake, PRODUCT_COUNT)

        print("Generating orders...")
        orders_df = generate_orders(fake, customers_df, ORDER_COUNT)

        print("Generating order items...")
        order_items_df = generate_order_items(fake, orders_df, products_df, ORDER_ITEM_COUNT)

        print("Generating reviews...")
        reviews_df = generate_reviews(fake, order_items_df, orders_df, REVIEW_COUNT)
    except Exception as error:
        raise RuntimeError(f"Data generation failed: {error}") from error

    save_dataframe(customers_df, "customers.csv")
    save_dataframe(products_df, "products.csv")
    save_dataframe(orders_df, "orders.csv")
    save_dataframe(order_items_df, "order_items.csv")
    save_dataframe(reviews_df, "reviews.csv")


if __name__ == "__main__":
    main()

