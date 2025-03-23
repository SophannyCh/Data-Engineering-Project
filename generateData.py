import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import re
from faker import Faker
import os

# Initialize Faker
fake = Faker()

# --- Configuration for Big Data Generation ---
num_customers = 100000       # 100K customers
num_products = 1000          # 1K products
num_sales = 1000000          # 2M sales transactions
num_payments = num_sales     # 2M payment records (1:1 with sales)
num_ads = 10000              # 10K marketing ads records
num_tickets = 5000           # 5K customer support records

# Probabilities for inserting missing values and duplicates
missing_prob = 0.05   # 5% chance to insert a missing value in a field
duplicate_prob = 0.05 # 5% of rows will be duplicated later

# --- Helper Functions ---
def maybe_missing(value):
    """Return np.nan with probability missing_prob."""
    return np.nan if random.random() < missing_prob else value

def add_duplicates(df, duplicate_frac):
    """Append duplicates of a random fraction of rows to the dataframe."""
    n_dup = int(len(df) * duplicate_frac)
    if n_dup > 0:
        dup_rows = df.sample(n=n_dup, random_state=42)
        df = pd.concat([df, dup_rows], ignore_index=True)
    return df

def generate_email_from_name(name):
    """Generate an email using the customer name followed by '@gmail.com'.
       Remove non-alphanumeric characters and convert to lower case."""
    username = re.sub(r'\W+', '', name).lower()
    return f"{username}@gmail.com"

def generate_phone():
    """Generate a phone number starting with 0 following the pattern '0XXX XXX XX' (9 digits total)."""
    digits = ''.join(random.choices("0123456789", k=8))
    return f"0{digits[:3]} {digits[3:6]} {digits[6:]}"

def random_date(start, end):
    """Generate a random date between start and end (as a date object)."""
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).date()

# --- New Product Generation Settings ---
adjectives = ["Super", "Ultra", "Mega", "Fashion", "Elegant", "Modern", "Classic", "Stylish", "Premium", "Affordable"]
product_types = [
    "Skincare", "Bag", "Kitchenware", "Clothes", "Beauty", "Shoes", "Food", 
    "Cosmetics", "Accessories", "Furniture", "Toys", "Groceries", "Snacks"
]

# --- Data Generation ---

# 1. Generate Customers Data
print("Generating Customers...")
customers_list = []
signup_start = datetime.today() - timedelta(days=730)  # 2 years ago
signup_end = datetime.today()
last_active_start = datetime.today() - timedelta(days=180)  # 6 months ago
last_active_end = datetime.today()
churn_threshold = datetime.today() - timedelta(days=90)

for i in range(1, num_customers + 1):
    cust_id = f"C{i:05d}"  # e.g., C00001
    name = maybe_missing(fake.name())
    gender = random.choice(["Male", "Female", "Other"])
    email = maybe_missing(generate_email_from_name(name)) if pd.notnull(name) else np.nan
    phone = maybe_missing(generate_phone())
    signup_date = maybe_missing(random_date(signup_start, signup_end))
    last_active_date = maybe_missing(random_date(last_active_start, last_active_end))
    location = maybe_missing(f"City_{random.randint(1,100)}")
    if pd.isna(last_active_date):
        churn_status = np.nan
    else:
        churn_status = "Active" if datetime.combine(last_active_date, datetime.min.time()) >= churn_threshold else "Churned"
    
    customers_list.append([cust_id, name, gender, email, phone, signup_date, last_active_date, location, churn_status])

df_customers = pd.DataFrame(customers_list, 
    columns=["customer_id", "name", "gender", "email", "phone_number", "signup_date", "last_active_date", "location", "churn_status"])
df_customers = add_duplicates(df_customers, duplicate_prob)
df_customers.to_csv("customers.csv", index=False)
print("Customers generated and saved to 'customers.csv'.")

# 2. Generate Products Data
print("Generating Products...")
products_list = []

for i in range(1, num_products + 1):
    prod_id = f"P{i:04d}"  # e.g., P0001
    prod_name = maybe_missing(f"{random.choice(adjectives)} {random.choice(product_types)}")
    category = maybe_missing(random.choice(product_types))
    price = maybe_missing(round(random.uniform(5, 1000), 2))
    stock_quantity = maybe_missing(random.randint(10, 500))
    supplier = maybe_missing(f"Supplier_{random.randint(1,50)}")
    rating = maybe_missing(round(random.uniform(3.0, 5.0), 1))
    reviews_count = maybe_missing(random.randint(5, 500))
    
    products_list.append([prod_id, prod_name, category, price, stock_quantity, supplier, rating, reviews_count])

df_products = pd.DataFrame(products_list, 
    columns=["product_id", "name", "category", "price", "stock_quantity", "supplier", "rating", "reviews_count"])
df_products = add_duplicates(df_products, duplicate_prob)
df_products.to_csv("products.csv", index=False)
print("Products generated and saved to 'products.csv'.")

# 3. Generate Sales Transactions Data
print("Generating Sales Transactions...")
sales_list = []
order_date_start = datetime.today() - timedelta(days=365)  # 1 year ago
order_date_end = datetime.today()

for i in range(1, num_sales + 1):
    order_id = f"R{i:07d}"  # e.g., R0000001
    customer_id = maybe_missing(random.choice(df_customers["customer_id"].tolist()))
    product_id = maybe_missing(random.choice(df_products["product_id"].tolist()))
    order_date = maybe_missing(random_date(order_date_start, order_date_end))
    total_amount = maybe_missing(round(random.uniform(10, 2000), 2))
    payment_id_placeholder = maybe_missing(f"PY{random.randint(1, num_sales):06d}")
    
    sales_list.append([order_id, customer_id, product_id, order_date, total_amount, payment_id_placeholder])

df_sales = pd.DataFrame(sales_list, 
    columns=["order_id", "customer_id", "product_id", "order_date", "total_amount", "payment_id"])
df_sales = add_duplicates(df_sales, duplicate_prob)
df_sales.to_csv("sales_transactions.csv", index=False)
print("Sales Transactions generated and saved to 'sales_transactions.csv'.")

# Collect payments data in a list
payments_data = []

# Split the process into smaller chunks
chunk_size = 100000  # This could be smaller to improve speed
output_file = "payments.csv"
header = ["payment_id", "order_id", "user_id", "payment_date", "payment_method", "total_paid", "transaction_fee", "payment_status"]

# Initialize file once with header
if not os.path.exists(output_file):
    with open(output_file, "w") as f:
        f.write(",".join(header) + "\n")

# 4 Generate payments in chunks and accumulate them in a list
for chunk in range(0, num_payments, chunk_size):
    payments_chunk = []
    start_index = chunk + 1
    end_index = min(chunk + chunk_size, num_payments)

    for i in range(start_index, end_index + 1):
        payment_id = f"PY{i:06d}"  # e.g., PY000001
        order_id = maybe_missing(random.choice(df_sales["order_id"].tolist()))
        # Convert a customer_id into a user_id by replacing 'C' with 'U'
        cust_id = random.choice(df_customers["customer_id"].tolist())
        user_id = f"U{cust_id[1:]}"
        payment_date = maybe_missing(random_date(order_date_start, order_date_end))
        pay_method = maybe_missing(random.choice(["Credit Card", "PayPal", "Bank Transfer", "Cash on Delivery"]))
        total_paid = maybe_missing(round(random.uniform(10, 2000), 2))
        transaction_fee = maybe_missing(round(random.uniform(0.5, 10), 2))
        pay_status = maybe_missing(random.choice(["Completed", "Pending", "Failed", "Refunded"]))

        # Skip record if critical fields are missing
        if order_id is None or payment_date is None or total_paid is None:
            continue

        payments_chunk.append([
            payment_id,
            order_id,
            user_id,
            payment_date,
            pay_method if pay_method is not None else "Unknown",
            total_paid,
            transaction_fee if transaction_fee is not None else 0,
            pay_status if pay_status is not None else "Pending"
        ])

    # Accumulate the current chunk of payments data
    payments_data.extend(payments_chunk)
    
    print(f"Chunk {(chunk // chunk_size) + 1} processed: records {start_index} to {end_index}")

# Convert the entire data to DataFrame once after all chunks
df_payments = pd.DataFrame(payments_data, columns=header)

# Write the full DataFrame to the CSV file in one go
df_payments.to_csv(output_file, mode='a', header=False, index=False)

print(f"Payments Data generation complete and saved to '{output_file}'.")


# 5. Generate Marketing & Ads Data
print("Generating Marketing & Ads Data...")
ads_list = []
ad_sources = ["Facebook", "Google Ads", "Instagram", "TikTok"]

for i in range(1, num_ads + 1):
    ad_source = maybe_missing(random.choice(ad_sources))
    campaign_name = maybe_missing(fake.catch_phrase())
    clicks = maybe_missing(random.randint(100, 10000))
    conversions = maybe_missing(random.randint(10, 5000))
    cost_per_click = maybe_missing(round(random.uniform(0.1, 5.0), 2))
    roas = maybe_missing(round(random.uniform(1.0, 5.0), 2))
    
    ads_list.append([ad_source, campaign_name, clicks, conversions, cost_per_click, roas])

df_ads = pd.DataFrame(ads_list, 
    columns=["ad_source", "campaign_name", "clicks", "conversions", "cost_per_click", "return_on_ad_spend"])
df_ads = add_duplicates(df_ads, duplicate_prob)
df_ads.to_csv("marketing_ads.csv", index=False)
print("Marketing & Ads Data generated and saved to 'marketing_ads.csv'.")

# 6. Generate Customer Support Data
print("Generating Customer Support Data...")
tickets_list = []

for i in range(1, num_tickets + 1):
    ticket_id = f"T{i:04d}"  # e.g., T0001
    customer_id = maybe_missing(random.choice(df_customers["customer_id"].tolist()))
    issue_type = maybe_missing(random.choice(["Refund Request", "Order Delay", "Product Inquiry", "Account Issue"]))
    response_time = maybe_missing(f"{random.randint(1,48)} hours")
    resolution_status = maybe_missing(random.choice(["Resolved", "Pending", "Escalated"]))
    feedback_rating = maybe_missing(round(random.uniform(1, 5), 1))
    
    tickets_list.append([ticket_id, customer_id, issue_type, response_time, resolution_status, feedback_rating])

df_tickets = pd.DataFrame(tickets_list, 
    columns=["ticket_id", "customer_id", "issue_type", "response_time", "resolution_status", "feedback_rating"])
df_tickets = add_duplicates(df_tickets, duplicate_prob)
df_tickets.to_csv("customer_support.csv", index=False)
print("Customer Support Data generated and saved to 'customer_support.csv'.")

print("Data generation complete. All CSV files have been saved.")