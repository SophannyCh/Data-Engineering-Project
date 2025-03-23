-- sql/create_tables.sql

-- Create Customers Table
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(50),
    signup_date DATE,
    last_active_date DATE,
    location VARCHAR(255),
    churn_status VARCHAR(50)
);

-- Create Sales Transactions Table
CREATE TABLE IF NOT EXISTS sales_transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    product_id INTEGER,
    order_date TIMESTAMP,
    total_amount NUMERIC,
    quantity INTEGER,
    payment_id VARCHAR(50),
    order_id INTEGER
    -- Optionally, add foreign key constraints here
);

-- Create Products Table
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(255),
    price NUMERIC,
    stock_quantity INTEGER,
    supplier VARCHAR(255),
    rating NUMERIC,
    reviews_count INTEGER
);

-- Create Payments Table
CREATE TABLE IF NOT EXISTS payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INTEGER,
    payment_date TIMESTAMP,
    total_paid NUMERIC,
    payment_method VARCHAR(50),
    transaction_fee NUMERIC,
    payment_status VARCHAR(50)
);

-- Create Marketing Ads Table
CREATE TABLE IF NOT EXISTS marketing_ads (
    ad_id SERIAL PRIMARY KEY,
    ad_source VARCHAR(255),
    campaign_name VARCHAR(255),
    clicks INTEGER,
    conversions INTEGER,
    cost_per_click NUMERIC,
    return_on_ad_spend NUMERIC
);

-- Create Customer Support Table
CREATE TABLE IF NOT EXISTS customer_support (
    ticket_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    issue_type VARCHAR(255),
    response_time VARCHAR(50),
    resolution_status VARCHAR(50),
    feedback_rating NUMERIC
);
