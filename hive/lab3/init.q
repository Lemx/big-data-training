CREATE DATABASE IF NOT EXISTS lab3;
USE lab3;

DELETE jars;
ADD JAR target/lab3-1.0.jar;
CREATE temporary function parse_user_agent AS 'UAParserUDF';

DROP TABLE IF EXISTS cities;

CREATE TABLE cities (
  id STRING, 
  name STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA INPATH '/big-data-training/hive/lab3/aux/city.en.txt' OVERWRITE INTO TABLE cities;


DROP TABLE IF EXISTS raw_logs;

CREATE TABLE raw_logs (
  line STRING
);

LOAD DATA INPATH '/big-data-training/hive/lab3/input' OVERWRITE INTO TABLE raw_logs;


DROP TABLE IF EXISTS logs;

CREATE TABLE logs
AS SELECT sub.city, sub.ua.type, sub.ua.family, sub.ua.os, sub.ua.device
FROM (SELECT c.name AS city, parse_user_agent(split(line, '\\t')[4]) AS ua 
      FROM raw_logs
      JOIN cities AS c
      ON split(line, '\\t')[7] = c.id) AS sub;
      