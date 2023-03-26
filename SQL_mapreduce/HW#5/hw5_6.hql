USE dualcore;

SELECT COUNT(*)
FROM (
    SELECT cust_id
    FROM orders O
    JOIN order_details D
    ON O.order_id = D.order_id
    JOIN products P
    ON D.prod_id = P.prod_id
    GROUP BY cust_id
    HAVING SUM(P.price) > 300000
) info;