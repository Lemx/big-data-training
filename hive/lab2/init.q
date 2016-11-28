CREATE DATABASE IF NOT EXISTS lab2;
USE lab2;

DROP TABLE IF EXISTS flights;

CREATE TABLE flights (
  year INT,
  month INT,
  day_of_month INT,
  day_of_week INT,
  dep_time INT,
  crs_dep_time INT,
  arr_time INT,
  crs_arr_time INT,
  carrier STRING,
  flight_num INT,
  tail_num INT,
  act_el_time INT,
  crs_el_time INT,
  air_time INT,
  arr_delay INT,
  dep_delay INT,
  origin STRING,
  dest STRING,
  dist INT,
  taxi_in INT,
  taxi_out INT,
  cancelled INT,
  cancellation_code STRING,
  diverted INT,
  carrier_dealy INT,
  weather_delay INT,
  nas_delay INT,
  sec_delay INT,
  late_aircraft_delay INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
(
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA INPATH '/big-data-training/hive/lab2/input/2007.csv' OVERWRITE INTO TABLE flights;



DROP TABLE IF EXISTS carriers;

CREATE TABLE carriers (
  code STRING,
  desc STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
(
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA INPATH '/big-data-training/hive/lab2/input/carriers.csv' OVERWRITE INTO TABLE carriers;



DROP TABLE IF EXISTS airports;

CREATE TABLE airports (
  iata STRING,
  airport STRING,
  city STRING,
  state STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
(
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA INPATH '/big-data-training/hive/lab2/input/airports.csv' OVERWRITE INTO TABLE airports;