"""
Run analytical SQL queries against the ecommerce SQLite database.

The script connects to `ecommerce.db`, executes several multi-table JOIN queries,
prints the results as formatted tables, and exports each result set to the
`results/` directory. Each query illustrates a different analytical perspective
on the synthetic data generated for the project.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from time import perf_counter
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd
from tabulate import tabulate

DATABASE_PATH = Path("ecommerce.db")
RESULTS_DIR = Path("results")

QueryResult = Tuple[str, int, float]
QueryRunner = Callable[[sqlite3.Connection], QueryResult]


def ensure_results_directory(directory: Path) -> None:
    """Ensure that the directory for storing query outputs exists."""
    directory.mkdir(parents=True, exist_ok=True)


def connect_database(path: Path) -> sqlite3.Connection:
    """Establish a read-write SQLite connection with FK checks enabled."""
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection


def execute_and_report(
    connection: sqlite3.Connection,
    name: str,
    sql: str,
    description: str,
    params: Optional[Tuple] = None,
) -> QueryResult:
    """
    Execute a SQL query, print tabulated output, and export to CSV.

    Parameters
    ----------
    connection:
        Active SQLite connection.
    name:
        Identifier used for logging and CSV filenames.
    sql:
        SQL statement to run.
    description:
        Human-friendly explanation of the business question addressed.
    params:
        Optional tuple of SQL parameters.
    """
    ensure_results_directory(RESULTS_DIR)
    print(f"\nRunning query: {name}")
    print(f"Description : {description}")

    start = perf_counter()
    try:
        dataframe = pd.read_sql_query(sql, connection, params=params)
    except sqlite3.DatabaseError as error:
        raise RuntimeError(f"Query '{name}' failed: {error}") from error
    duration = perf_counter() - start
    row_count = len(dataframe)

    print(f"Execution time: {duration:.2f} seconds | Rows: {row_count}")
    if row_count == 0:
        print("No records found for this query.")
    else:
        print(tabulate(dataframe, headers="keys", tablefmt="psql", showindex=False))

    output_path = RESULTS_DIR / f"{name}.csv"
    dataframe.to_csv(output_path, index=False)
    print(f"Results exported to {output_path.resolve()}")
    return name, row_count, duration


def top_customers_by_revenue(connection: sqlite3.Connection) -> QueryResult:
    """
    Identify the top 20 customers by total spending to highlight revenue drivers.
    """
    sql = """
        SELECT
            c.customer_id,
            c.first_name || ' ' || c.last_name AS customer_name,
            c.email,
            COUNT(DISTINCT o.order_id) AS total_orders,
            ROUND(SUM(oi.subtotal), 2) AS total_revenue
        FROM customers AS c
        INNER JOIN orders AS o ON o.customer_id = c.customer_id
        INNER JOIN order_items AS oi ON oi.order_id = o.order_id
        GROUP BY c.customer_id, customer_name, c.email
        ORDER BY total_revenue DESC
        LIMIT 20;
    """
    description = "Who are our top 20 customers by total spending?"
    return execute_and_report(connection, "top_customers_by_revenue", sql, description)


def product_performance_with_reviews(connection: sqlite3.Connection) -> QueryResult:
    """
    Evaluate each product's sales contribution and customer sentiment via reviews.
    """
    sql = """
        WITH order_stats AS (
            SELECT
                product_id,
                SUM(quantity) AS total_units_sold,
                ROUND(SUM(subtotal), 2) AS total_revenue
            FROM order_items
            GROUP BY product_id
        ),
        review_stats AS (
            SELECT
                product_id,
                ROUND(AVG(rating), 2) AS average_rating,
                COUNT(*) AS review_count
            FROM reviews
            GROUP BY product_id
        )
        SELECT
            p.product_id,
            p.product_name,
            p.category,
            COALESCE(o.total_units_sold, 0) AS total_units_sold,
            COALESCE(o.total_revenue, 0) AS total_revenue,
            COALESCE(r.average_rating, 0) AS average_rating,
            COALESCE(r.review_count, 0) AS review_count
        FROM products AS p
        LEFT JOIN order_stats AS o ON o.product_id = p.product_id
        LEFT JOIN review_stats AS r ON r.product_id = p.product_id
        ORDER BY total_revenue DESC, p.product_name ASC;
    """
    description = "How is each product performing in sales and ratings?"
    return execute_and_report(
        connection,
        "product_performance_with_reviews",
        sql,
        description,
    )


def complete_order_details(connection: sqlite3.Connection) -> QueryResult:
    """
    Retrieve recent order line-items with associated customer and product context.
    """
    sql = """
        SELECT
            o.order_id,
            o.order_date,
            c.first_name || ' ' || c.last_name AS customer_name,
            p.product_name,
            oi.quantity,
            oi.subtotal,
            o.status
        FROM orders AS o
        INNER JOIN customers AS c ON c.customer_id = o.customer_id
        INNER JOIN order_items AS oi ON oi.order_id = o.order_id
        INNER JOIN products AS p ON p.product_id = oi.product_id
        WHERE o.order_date >= DATE((SELECT MAX(order_date) FROM orders), '-30 day')
        ORDER BY o.order_date DESC, o.order_id DESC
        LIMIT 100;
    """
    description = "Get full order details (last 30 days) including customer and product info."
    return execute_and_report(
        connection,
        "complete_order_details",
        sql,
        description,
    )


def category_sales_summary(connection: sqlite3.Connection) -> QueryResult:
    """
    Compare product categories by revenue, volume, and average order value.
    """
    sql = """
        SELECT
            p.category,
            COUNT(DISTINCT oi.order_id) AS total_orders,
            SUM(oi.quantity) AS total_units_sold,
            ROUND(SUM(oi.subtotal), 2) AS total_revenue,
            ROUND(
                CASE
                    WHEN COUNT(DISTINCT oi.order_id) = 0 THEN 0
                    ELSE SUM(oi.subtotal) / COUNT(DISTINCT oi.order_id)
                END,
                2
            ) AS average_order_value
        FROM products AS p
        INNER JOIN order_items AS oi ON oi.product_id = p.product_id
        GROUP BY p.category
        ORDER BY total_revenue DESC;
    """
    description = "Which product categories generate the most revenue?"
    return execute_and_report(
        connection,
        "category_sales_summary",
        sql,
        description,
    )


def customer_engagement_metrics(connection: sqlite3.Connection) -> QueryResult:
    """
    Surface customers with high purchase frequency and active review participation.
    """
    sql = """
        WITH order_metrics AS (
            SELECT
                c.customer_id,
                SUM(oi.subtotal) AS total_spent,
                COUNT(DISTINCT o.order_id) AS total_orders
            FROM customers AS c
            INNER JOIN orders AS o ON o.customer_id = c.customer_id
            INNER JOIN order_items AS oi ON oi.order_id = o.order_id
            GROUP BY c.customer_id
        ),
        review_metrics AS (
            SELECT
                customer_id,
                COUNT(*) AS total_reviews,
                ROUND(AVG(rating), 2) AS average_rating
            FROM reviews
            GROUP BY customer_id
        )
        SELECT
            c.customer_id,
            c.first_name || ' ' || c.last_name AS customer_name,
            om.total_orders,
            ROUND(om.total_spent, 2) AS total_spent,
            ROUND(om.total_spent / om.total_orders, 2) AS average_order_value,
            COALESCE(rm.total_reviews, 0) AS total_reviews,
            COALESCE(rm.average_rating, 0) AS average_rating_given
        FROM customers AS c
        INNER JOIN order_metrics AS om ON om.customer_id = c.customer_id
        LEFT JOIN review_metrics AS rm ON rm.customer_id = c.customer_id
        WHERE om.total_orders >= 2
        ORDER BY total_spent DESC
        LIMIT 50;
    """
    description = "Which customers are most engaged (repeat purchases plus reviews)?"
    return execute_and_report(
        connection,
        "customer_engagement_metrics",
        sql,
        description,
    )


def summarize_runs(results: List[QueryResult]) -> None:
    """Print a tabulated summary of all executed queries."""
    if not results:
        print("No queries were executed.")
        return
    summary_df = pd.DataFrame(
        [
            {"query": name, "rows": rows, "seconds": round(seconds, 2)}
            for name, rows, seconds in results
        ]
    )
    print("\nSummary of executed queries:")
    print(tabulate(summary_df, headers="keys", tablefmt="psql", showindex=False))


def main() -> None:
    """Connect to the database and run the full analytical workload."""
    if not DATABASE_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DATABASE_PATH.resolve()}. "
            "Run data generation and ingestion scripts first."
        )

    query_functions: List[QueryRunner] = [
        top_customers_by_revenue,
        product_performance_with_reviews,
        complete_order_details,
        category_sales_summary,
        customer_engagement_metrics,
    ]

    results: List[QueryResult] = []
    try:
        with connect_database(DATABASE_PATH) as connection:
            for runner in query_functions:
                results.append(runner(connection))
    except Exception as error:
        raise RuntimeError(f"Analysis failed: {error}") from error

    summarize_runs(results)
    print("\nAnalysis completed successfully.")


if __name__ == "__main__":
    main()

