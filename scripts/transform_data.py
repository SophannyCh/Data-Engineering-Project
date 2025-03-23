# scripts/transform_data.py

import pandas as pd

# ---------------------
# Cleaning Functions
# ---------------------

def clean_customers_sup(df, table_name):
    if df is None:
        return None

    print(f"🔹 Cleaning {table_name} data...")
    if table_name == "customer_support":
        df.dropna(subset=["ticket_id"], inplace=True)
        df.dropna(subset=["customer_id"], inplace=True)
        df["issue_type"].fillna("Unknown", inplace=True)
        df["response_time"].fillna("Unknown", inplace=True)
        df["resolution_status"].fillna("Pending", inplace=True)
        median_rating = df["feedback_rating"].median()
        df["feedback_rating"].fillna(median_rating, inplace=True)
    print(f"✅ Cleaned: {table_name}")
    return df

def clean_customer(df, table_name):
    if df is None:
        return None

    print(f"🔹 Cleaning {table_name} data...")
    if table_name == "customers":
        df.dropna(subset=["signup_date"], inplace=True)
        df["name"].fillna("Unknown", inplace=True)
        df["email"].fillna("Unknown", inplace=True)
        df["phone_number"].fillna("000-000000", inplace=True)
        df["last_active_date"].fillna(df["signup_date"], inplace=True)
        df["location"].fillna("Unknown", inplace=True)
        df["churn_status"].fillna("Active", inplace=True)
        df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
        df["last_active_date"] = pd.to_datetime(df["last_active_date"], errors="coerce")
    print(f"✅ Cleaned: {table_name}")
    return df

def clean_marketing_ads(df, table_name):
    if df is None:
        return None

    print(f"🔹 Cleaning {table_name} data...")
    if table_name == "marketing_ads":
        df["ad_source"].fillna("Unknown", inplace=True)
        df["campaign_name"].fillna("Unnamed Campaign", inplace=True)
        df["clicks"].fillna(0, inplace=True)
        df["conversions"].fillna(0, inplace=True)
        mean_cpc = df["cost_per_click"].mean()
        df["cost_per_click"].fillna(mean_cpc, inplace=True)
        median_roas = df["return_on_ad_spend"].median()
        df["return_on_ad_spend"].fillna(median_roas, inplace=True)
    print(f"✅ Cleaned: {table_name}")
    return df

def clean_payments(df, table_name):
    if df is None:
        return None

    print(f"🔹 Cleaning {table_name} data...")
    if table_name == "payments":
        df.dropna(subset=["order_id"], inplace=True)
        df.dropna(subset=["payment_date"], inplace=True)
        df.dropna(subset=["total_paid"], inplace=True)
        df["payment_method"].fillna("Unknown", inplace=True)
        median_fee = df["transaction_fee"].median()
        df["transaction_fee"].fillna(median_fee, inplace=True)
        df["payment_status"].fillna("Pending", inplace=True)
        df["payment_date"] = pd.to_datetime(df["payment_date"], errors="coerce")
    print(f"✅ Cleaned: {table_name}")
    return df

def clean_products(df, table_name):
    if df is None:
        return None

    print(f"🔹 Cleaning {table_name} data...")
    if table_name == "products":
        df["name"].fillna("Unknown Product", inplace=True)
        df["category"].fillna("Other", inplace=True)
        median_price = df["price"].median()
        df["price"].fillna(median_price, inplace=True)
        df["stock_quantity"].fillna(0, inplace=True)
        df["supplier"].fillna("Unknown Supplier", inplace=True)
        mean_rating = df["rating"].mean()
        df["rating"].fillna(mean_rating, inplace=True)
        df["reviews_count"].fillna(0, inplace=True)
    print(f"✅ Cleaned: {table_name}")
    return df

def clean_sales_transaction(df, table_name):
    if df is None:
        return None

    print(f"🔹 Cleaning {table_name} data...")
    if table_name == "sales_transactions":
        df.dropna(subset=["customer_id"], inplace=True)
        df.dropna(subset=["product_id"], inplace=True)
        df.dropna(subset=["order_date"], inplace=True)
        df.dropna(subset=["total_amount"], inplace=True)
        df["payment_id"].fillna("Unknown", inplace=True)
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    print(f"✅ Cleaned: {table_name}")
    return df

# ---------------------
# Transformation Function
# ---------------------

def transform_all(customers, sales, products, payments, marketing_ads, customer_support):
    print("🔄 Starting full transformation process...")

    # Clean each dataset
    customers = clean_customer(customers, "customers")
    sales = clean_sales_transaction(sales, "sales_transactions")
    products = clean_products(products, "products")
    payments = clean_payments(payments, "payments")
    marketing_ads = clean_marketing_ads(marketing_ads, "marketing_ads")
    customer_support = clean_customers_sup(customer_support, "customer_support")

    # Create Sales Mart: Merge sales with customers, products, and payments
    print("🔁 Merging datasets for sales_mart...")
    sales_mart = sales.merge(customers, on="customer_id", how="left")
    sales_mart = sales_mart.merge(products, on="product_id", how="left")
    sales_mart = sales_mart.merge(payments, on="order_id", how="left")
    
    # Calculate revenue (assuming columns 'quantity' and 'price' exist)
    if "quantity" in sales_mart.columns and "price" in sales_mart.columns:
        sales_mart["revenue"] = sales_mart["quantity"] * sales_mart["price"]
    else:
        sales_mart["revenue"] = None

    # Flag churned customers: customers with last order older than 30 days from the most recent order
    print("📆 Flagging churned customers...")
    if "order_date" in sales_mart.columns:
        sales_mart["order_date"] = pd.to_datetime(sales_mart["order_date"], errors="coerce")
        last_date = sales_mart["order_date"].max()
        customer_last_order = sales_mart.groupby("customer_id")["order_date"].max()
        churned_customers = customer_last_order[customer_last_order < (last_date - pd.Timedelta(days=30))].index
        sales_mart["churned"] = sales_mart["customer_id"].isin(churned_customers)
    else:
        sales_mart["churned"] = False

    # Create Marketing Mart: Process marketing ads data (calculate cost)
    print("📊 Creating marketing_mart...")
    marketing_mart = marketing_ads.copy()
    if "clicks" in marketing_mart.columns and "cost_per_click" in marketing_mart.columns:
        marketing_mart["cost"] = marketing_mart["clicks"] * marketing_mart["cost_per_click"]
    else:
        marketing_mart["cost"] = None

    # Create Support Mart: For customer support data, simply copy cleaned data
    print("💬 Creating support_mart...")
    support_mart = customer_support.copy()

    print("✅ Transformation complete.")
    return sales_mart, marketing_mart, support_mart

if __name__ == "__main__":
    print("This module provides the transform_all() function for ETL processing.")