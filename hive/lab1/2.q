USE lab1;

SELECT SUM(sub.cnt) AS NYC_Total
FROM (
	SELECT COUNT(*) AS cnt 
	FROM airports AS a
	JOIN flights AS f
	ON a.iata = f.origin
	WHERE a.city = "New York" AND f.month = 6
	UNION
	SELECT COUNT(*) AS cnt
	FROM airports AS a
	JOIN flights AS f
	ON a.iata = f.dest
	WHERE a.city = "New York" AND f.month = 6
) AS sub;