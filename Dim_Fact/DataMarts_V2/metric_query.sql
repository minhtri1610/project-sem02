--table dim_revenue_per_cus
WITH customer_revenue AS (
    SELECT c.customer_id, SUM(od.unit_price * od.quantity) AS revenue, AVG(od.unit_price * od.quantity) AS average_order_value,  date_part('year', o.order_date) as year
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_details od ON o.order_id = od.order_id
    GROUP BY c.customer_id, year
),
quartiles AS (
    SELECT
        (SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY revenue) FROM customer_revenue) AS Q1,
        (SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY revenue) FROM customer_revenue) AS Q3
)
SELECT
    customer_id,
    revenue,
    average_order_value,
	year,
    CASE
        WHEN revenue >= (SELECT Q3 FROM quartiles) THEN 'High-Value Buyer'
        ELSE 'Low-Value Buyer'
    END AS high_low_byers
FROM
    customer_revenue;


--table dim_region_w_customer
SELECT C
	.region,
	COUNT ( C.customer_id ) AS count_customer
FROM
	customers C
GROUP BY
	C.region;

--table dim_metric
select drpc.year, SUM(drpc.revenue) as total_sale, (tbl_top_25.total_revenue_top / SUM(drpc.revenue)) * 100 as contribution_precent
from dim_revenue_per_cus as drpc
LEFT JOIN (
    SELECT SUM(total) AS total_revenue_top , year
    FROM (
        select SUM(revenue) as total, "year"
        from dim_revenue_per_cus group by year, customer_id ORDER BY total DESC LIMIT 25
        ) subquery
    GROUP BY year
    ORDER BY year
) as tbl_top_25 ON tbl_top_25.year = drpc.year
group by drpc.year, tbl_top_25.total_revenue_top ORDER BY drpc.year;




-- lấy tổng chi tiêu của top 25 khách hàng theo từng năm
-- SELECT SUM(total) AS total_revenue, "year"
-- FROM (
-- 	select SUM(revenue) as total, "year"
-- 	from dim_revenue_per_cus group by year, customer_id ORDER BY total DESC LIMIT 25
-- 	) subquery
-- GROUP BY "year"
-- ORDER BY "year";
