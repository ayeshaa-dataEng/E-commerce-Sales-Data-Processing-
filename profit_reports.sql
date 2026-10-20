-- Run after notebook 01. Change the catalog/schema to your output destination.
SELECT year, SUM(total_profit) AS total_profit FROM main.default.profit_aggregate GROUP BY year ORDER BY year;
SELECT year, category, SUM(total_profit) AS total_profit FROM main.default.profit_aggregate GROUP BY year, category ORDER BY year, category;
SELECT customer_id, customer_name, SUM(total_profit) AS total_profit FROM main.default.profit_aggregate GROUP BY customer_id, customer_name ORDER BY customer_id;
SELECT customer_id, customer_name, year, SUM(total_profit) AS total_profit FROM main.default.profit_aggregate GROUP BY customer_id, customer_name, year ORDER BY customer_id, year;
