CREATE DATABASE IF NOT EXISTS lab4;
USE lab4;

CREATE TABLE IF NOT EXISTS settings (hostIp STRING, type INT, value DOUBLE, period BIGINT);
INSERT INTO settings VALUES (NULL, 1, 5.0, 10), (NULL, 2, 30, 60);

CREATE EXTERNAL TABLE IF NOT EXISTS statistics_by_hour (
  time TIMESTAMP,
  hostIp STRING,
  trafficConsumed DOUBLE,
  averageSpeed DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/big-data-training/spark/lab4/stats';



-- queries used to retrieve data
-- due to hardcoded time may or may not work with other experiments

--SELECT HOUR(time) AS hour, hostIp AS ip, averageSpeed AS mb_per_min
--FROM statistics_by_hour
--WHERE HOUR(time) = 10
--ORDER BY mb_per_min DESC
--LIMIT 3;
--
--SELECT HOUR(time) AS hour, hostIp AS ip, averageSpeed AS mb_per_min
--FROM statistics_by_hour
--WHERE HOUR(time) = 18
--ORDER BY mb_per_min DESC
--LIMIT 3;