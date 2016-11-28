USE lab2;

SELECT c.desc AS airport, COUNT(*) AS flights, CONCAT_WS(', ', COLLECT_SET(a.city)) AS cities
FROM carriers AS c
JOIN flights AS f
ON c.code = f.carrier
JOIN airports AS a
ON a.iata = f.origin
WHERE f.cancelled = 1 AND f.cancellation_code = 'A'
GROUP BY c.desc
HAVING flights > 1
SORT BY flights DESC;