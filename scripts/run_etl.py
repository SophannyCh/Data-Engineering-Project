# run_etl.py

from scripts.extract_data import extract_all_data
from scripts.transform_data import transform_all
from scripts.load_to_postgres import load_to_postgres

if __name__ == "__main__":
    # Extract
    customers, sales, products, payments, marketing_ads, customer_support = extract_all_data()

    # Transform
    sales_mart, marketing_mart, support_mart = transform_all(
        customers, sales, products, payments, marketing_ads, customer_support
    )

    # Load to PostgreSQL (update db_url in load_to_postgres.py as needed)
    load_to_postgres(sales_mart, "sales_mart")
    load_to_postgres(marketing_mart, "marketing_mart")
    load_to_postgres(support_mart, "support_mart")

    print("ETL process complete.")
