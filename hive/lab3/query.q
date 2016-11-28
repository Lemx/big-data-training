SELECT c.name, d.device, d.countD, b.browser, b.countB, o.os, o.countO
FROM cities AS c
JOIN (SELECT sub2.city AS city, sub3.device AS device, sub2.mx AS countD
	FROM
	(
		SELECT sub1.city AS city, MAX(sub1.count) AS mx
		FROM 
		(
			SELECT city, device, COUNT(device) AS count
			FROM logs GROUP BY city, device
		) AS sub1 
		GROUP BY sub1.city
		) AS sub2
	JOIN (
			SELECT city, device, COUNT(device) AS count
			FROM logs GROUP BY city, device
		) AS sub3
	ON sub2.mx = sub3.count AND sub2.city = sub3.city) AS d
ON c.name = d.city
JOIN (SELECT sub2.city AS city, sub3.family AS browser, sub2.mx AS countB
	FROM
	(
		SELECT sub1.city AS city, MAX(sub1.count) AS mx
		FROM 
		(
			SELECT city, family, COUNT(family) AS count
			FROM logs GROUP BY city, family
		) AS sub1 
		GROUP BY sub1.city
		) AS sub2
	JOIN (
			SELECT city, family, COUNT(family) AS count
			FROM logs GROUP BY city, family
		) AS sub3
	ON sub2.mx = sub3.count AND sub2.city = sub3.city) AS b
ON c.name = b.city
JOIN (SELECT sub2.city AS city, sub3.os AS os, sub2.mx AS countO
	FROM
	(
		SELECT sub1.city AS city, MAX(sub1.count) AS mx
		FROM 
		(
			SELECT city, os, COUNT(os) AS count
			FROM logs GROUP BY city, os
		) AS sub1 
		GROUP BY sub1.city
		) AS sub2
	JOIN (
			SELECT city, os, COUNT(os) AS count
			FROM logs GROUP BY city, os
		) AS sub3
	ON sub2.mx = sub3.count AND sub2.city = sub3.city) AS o
ON c.name = o.city
SORT BY c.name;