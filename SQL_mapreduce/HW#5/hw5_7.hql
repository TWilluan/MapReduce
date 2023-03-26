USE dualcore;

SELECT C.cust_id
FROM customers C
FULL OUTER JOIN orders O
ON C.cust_id = O.cust_id
WHERE order_id IS NULL;