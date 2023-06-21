/* Total Revenue per Customer*/
SELECT c.customer_id, SUM(od.unit_price * od.quantity) AS TotalRevenue
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_details od ON o.order_id = od.order_id
GROUP BY c.customer_id;

/*High,Low-Value Buyers*/
WITH customer_revenue AS (
    SELECT c.customer_id, SUM(od.unit_price * od.quantity) AS TotalRevenue
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_details od ON o.order_id = od.order_id
    GROUP BY c.customer_id
),
quartiles AS (
    SELECT
        (SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY TotalRevenue) FROM customer_revenue) AS Q1,
        (SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY TotalRevenue) FROM customer_revenue) AS Q3
)
SELECT
    customer_id,
    TotalRevenue,
    CASE
        WHEN TotalRevenue >= (SELECT Q3 FROM quartiles) THEN 'High-Value Buyer'
        ELSE 'Low-Value Buyer'
    END AS BuyerSegment
FROM
    customer_revenue;

/*Average Order Value per Customer*/
SELECT c.customer_id, AVG(od.unit_price * od.quantity) AS AvgOrderValue
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_details od ON o.order_id = od.order_id
GROUP BY c.customer_id;

/*Product Preference*/
/*based on northwind data, mostly food related things*/
SELECT c.customer_id, cat.category_name AS CategoryName,
       CASE
           WHEN cat.category_name IN ('Dairy Products', 'Beverages', 'Grain/Cereals', 'Meat/Poultry', 'Seafood', 'Confections') THEN 'Food & Beverages Buyers'
           WHEN cat.category_name = 'Condiments' THEN 'Condiments Shoppers'
           ELSE 'Other'
       END AS CustomerSegment
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_details od ON o.order_id = od.order_id
JOIN products p ON od.product_id = p.product_id
JOIN categories cat ON p.category_id = cat.category_id
GROUP BY c.customer_id, cat.category_name;

/*Customer Segmentation by Region*/
SELECT c.region, COUNT(c.customer_id) AS CustomerCount
FROM customers c
GROUP BY c.region;

/*Most Frequent Employee Interaction per Customer*/
SELECT e.employee_id, e.first_name, e.last_name, COUNT(o.order_id) AS InteractionCount
FROM orders o
JOIN employees e ON o.employee_id = e.employee_id
GROUP BY e.employee_id, e.first_name, e.last_name
ORDER BY InteractionCount DESC;

/*Customer Recency*/
SELECT
  o.customer_id,
  CASE
    WHEN DATE_PART('day', TIMESTAMP '1998-05-06'::DATE - DATE_TRUNC('day', MAX(o.order_date))) <= 30 THEN 'Recent Customer'
    WHEN DATE_PART('day', TIMESTAMP '1998-05-06'::DATE - DATE_TRUNC('day', MAX(o.order_date))) <= 90 THEN 'Active Customer'
    ELSE 'Inactive Customer'
  END AS CustomerStatus
FROM orders o
GROUP BY o.customer_id;

/*Repeat Purchases*/
SELECT customer_id, COUNT(*) AS repeat_purchases
FROM orders
GROUP BY customer_id;

/*New Customers*/
SELECT COUNT(DISTINCT customer_id) AS new_customers
FROM orders
WHERE order_date BETWEEN '1997-11-01' AND '1997-12-31';

/*Customer Churn*/
SELECT COUNT(DISTINCT customer_id) AS churned_customers
FROM customers
WHERE customer_id NOT IN (
    SELECT DISTINCT customer_id
    FROM orders
    WHERE order_date BETWEEN '1997-11-01' AND '1997-12-31'
);
/*list*/
SELECT c.customer_id, c.contact_name
FROM customers c
WHERE c.customer_id NOT IN (
    SELECT DISTINCT o.customer_id
    FROM orders o
    WHERE o.order_date BETWEEN '1997-11-01' AND '1997-12-31'
);



