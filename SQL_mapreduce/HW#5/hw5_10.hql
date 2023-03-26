USE dualcore;

SELECT prod_id, message
FROM ratings
WHERE message LIKE '%ten times more%';