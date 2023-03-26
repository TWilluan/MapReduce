USE dualcore;

SELECT EXPLODE(NGRAMS(SENTENCES(LOWER(message)), 3, 5)) as tri_ngram
FROM ratings
WHERE prod_id = 1274673;