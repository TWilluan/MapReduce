USE dualcore;

DROP TABLE IF EXISTS ratings;

CREATE TABLE ratings
(
    posted TIMESTAMP,
    cust_id INT,
    prod_id INT,
    rating TINYINT,
    message STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/hadoop-data/ratings_2012.txt'
INTO TABLE ratings;

LOAD DATA LOCAL INPATH '/hadoop-data/ratings_2013.txt'
INTO TABLE ratings;

SELECT prod_id
FROM ratings
GROUP BY prod_id
HAVING COUNT(prod_id) >= 50
ORDER BY AVG(rating) ASC
LIMIT 1;