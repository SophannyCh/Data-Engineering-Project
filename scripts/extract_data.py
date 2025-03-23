# scripts/extract_data.py

import pandas as pd

def extract_all_data(data_path="../Data"):
    print("[Extract] Reading CSV files from", data_path)
    
    customers = pd.read_csv(f"{data_path}customers.csv")
    sales = pd.read_csv(f"{data_path}sales_transactions.csv")
    products = pd.read_csv(f"{data_path}products.csv")
    payments = pd.read_csv(f"{data_path}payments.csv")
    marketing_ads = pd.read_csv(f"{data_path}marketing_ads.csv")
    customer_support = pd.read_csv(f"{data_path}customer_support.csv")
    
    print("[Extract] Done reading CSV files.")
    return customers, sales, products, payments, marketing_ads, customer_support

if __name__ == "__main__":
    extract_all_data()
