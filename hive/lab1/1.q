USE lab1;

SELECT c.desc AS Carrier, COUNT(*) AS Flights
FROM carriers AS c 
INNER JOIN flights AS f
ON c.code = f.carrier
GROUP BY c.desc;