USE lab1;

SELECT sub.airport AS airport, SUM(sub.cnt) AS flights
FROM (
	SELECT a.airport AS airport, COUNT(*) AS cnt
	FROM airports AS a
	JOIN flights AS f
	ON a.iata = f.origin
	WHERE a.country = "USA" AND f.month IN (6, 7, 8)
	GROUP BY a.airport
	UNION
	SELECT a.airport AS airport, COUNT(*) AS cnt
	FROM airports AS a
	JOIN flights AS f
	ON a.iata = f.dest
	WHERE a.country = "USA" AND f.month IN (6, 7, 8)
	GROUP BY a.airport
) AS sub
GROUP BY airport
SORT BY flights DESC
LIMIT 5;