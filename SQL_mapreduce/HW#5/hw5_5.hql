USE dualcore;

SELECT COUNT(*)
FROM orders o
JOIN order_details d
ON o.order_id = d.order_id
WHERE cust_id = '1071189';