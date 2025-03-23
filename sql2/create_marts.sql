-- sql/create_marts.sql

-- Sales Mart: Combines sales, customers, products, and payments data
CREATE OR REPLACE VIEW sales_mart AS
SELECT 
    s.transaction_id,
    s.customer_id,
    c.name AS customer_name,
    s.product_id,
    p.name AS product_name,
    s.order_date,
    s.quantity,
    p.price,
    s.quantity * p.price AS revenue,
    s.payment_id,
    pm.payment_date,
    pm.payment_method,
    c.churn_status
FROM sales_transactions s
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN products p ON s.product_id = p.product_id
LEFT JOIN payments pm ON s.order_id = pm.order_id;

-- Marketing Mart: Summarizes marketing ads data
CREATE OR REPLACE VIEW marketing_mart AS
SELECT 
    ad_id,
    ad_source,
    campaign_name,
    clicks,
    conversions,
    cost_per_click,
    return_on_ad_spend,
    clicks * cost_per_click AS cost
FROM marketing_ads;

-- Support Mart: Provides customer support details for analysis
CREATE OR REPLACE VIEW support_mart AS
SELECT 
    ticket_id,
    customer_id,
    issue_type,
    response_time,
    resolution_status,
    feedback_rating
FROM customer_support;
