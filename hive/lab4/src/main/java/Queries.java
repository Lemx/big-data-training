import java.util.ArrayList;
import java.util.Arrays;

public class Queries {

    public static final ArrayList<String> createFlights = new ArrayList<>(Arrays.asList(
            "CREATE TABLE flights%s (\n" +
                    "  year INT,\n" +
                    "  month INT,\n" +
                    "  day_of_month INT,\n" +
                    "  day_of_week INT,\n" +
                    "  dep_time INT,\n" +
                    "  crs_dep_time INT,\n" +
                    "  arr_time INT,\n" +
                    "  crs_arr_time INT,\n" +
                    "  carrier STRING,\n" +
                    "  flight_num INT,\n" +
                    "  tail_num INT,\n" +
                    "  act_el_time INT,\n" +
                    "  crs_el_time INT,\n" +
                    "  air_time INT,\n" +
                    "  arr_delay INT,\n" +
                    "  dep_delay INT,\n" +
                    "  origin STRING,\n" +
                    "  dest STRING,\n" +
                    "  dist INT,\n" +
                    "  taxi_in INT,\n" +
                    "  taxi_out INT,\n" +
                    "  cancelled INT,\n" +
                    "  cancellation_code STRING,\n" +
                    "  diverted INT,\n" +
                    "  carrier_dealy INT,\n" +
                    "  weather_delay INT,\n" +
                    "  nas_delay INT,\n" +
                    "  sec_delay INT,\n" +
                    "  late_aircraft_delay INT\n" +
                    ")\n" +
                    "%s\n" +
                    "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n" +
                    "WITH SERDEPROPERTIES \n" +
                    "(\n" +
                    "    \"separatorChar\" = \",\",\n" +
                    "    \"quoteChar\"     = \"\\\"\"\n" +
                    ")\n" +
                    "STORED AS TEXTFILE\n" +
                    "TBLPROPERTIES(\"skip.header.line.count\"=\"1\")",
            "CREATE TABLE flights%s (\n" +
                    "  year INT,\n" +
                    "  day_of_month INT,\n" +
                    "  day_of_week INT,\n" +
                    "  dep_time INT,\n" +
                    "  crs_dep_time INT,\n" +
                    "  arr_time INT,\n" +
                    "  crs_arr_time INT,\n" +
                    "  carrier STRING,\n" +
                    "  flight_num INT,\n" +
                    "  tail_num INT,\n" +
                    "  act_el_time INT,\n" +
                    "  crs_el_time INT,\n" +
                    "  air_time INT,\n" +
                    "  arr_delay INT,\n" +
                    "  dep_delay INT,\n" +
                    "  origin STRING,\n" +
                    "  dest STRING,\n" +
                    "  dist INT,\n" +
                    "  taxi_in INT,\n" +
                    "  taxi_out INT,\n" +
                    "  cancelled INT,\n" +
                    "  cancellation_code STRING,\n" +
                    "  diverted INT,\n" +
                    "  carrier_dealy INT,\n" +
                    "  weather_delay INT,\n" +
                    "  nas_delay INT,\n" +
                    "  sec_delay INT,\n" +
                    "  late_aircraft_delay INT\n" +
                    ")\n" +
                    "%s\n" +
                    "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n" +
                    "WITH SERDEPROPERTIES \n" +
                    "(\n" +
                    "    \"separatorChar\" = \",\",\n" +
                    "    \"quoteChar\"     = \"\\\"\"\n" +
                    ")\n" +
                    "STORED AS TEXTFILE\n" +
                    "TBLPROPERTIES(\"skip.header.line.count\"=\"1\")",
            "CREATE TABLE flights%s (\n" +
                    "  year INT,\n" +
                    "  month INT,\n" +
                    "  day_of_month INT,\n" +
                    "  day_of_week INT,\n" +
                    "  dep_time INT,\n" +
                    "  crs_dep_time INT,\n" +
                    "  arr_time INT,\n" +
                    "  crs_arr_time INT,\n" +
                    "  carrier STRING,\n" +
                    "  flight_num INT,\n" +
                    "  tail_num INT,\n" +
                    "  act_el_time INT,\n" +
                    "  crs_el_time INT,\n" +
                    "  air_time INT,\n" +
                    "  arr_delay INT,\n" +
                    "  dep_delay INT,\n" +
                    "  origin STRING,\n" +
                    "  dest STRING,\n" +
                    "  dist INT,\n" +
                    "  taxi_in INT,\n" +
                    "  taxi_out INT,\n" +
                    "  cancelled INT,\n" +
                    "  cancellation_code STRING,\n" +
                    "  diverted INT,\n" +
                    "  carrier_dealy INT,\n" +
                    "  weather_delay INT,\n" +
                    "  nas_delay INT,\n" +
                    "  sec_delay INT,\n" +
                    "  late_aircraft_delay INT\n" +
                    ")\n" +
                    "%s\n" +
                    "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n" +
                    "WITH SERDEPROPERTIES \n" +
                    "(\n" +
                    "    \"separatorChar\" = \",\",\n" +
                    "    \"quoteChar\"     = \"\\\"\"\n" +
                    ")\n" +
                    "STORED AS TEXTFILE\n" +
                    "TBLPROPERTIES(\"skip.header.line.count\"=\"1\")",
            "CREATE TABLE flights%s (\n" +
                    "  year INT,\n" +
                    "  month INT,\n" +
                    "  day_of_month INT,\n" +
                    "  day_of_week INT,\n" +
                    "  dep_time INT,\n" +
                    "  crs_dep_time INT,\n" +
                    "  arr_time INT,\n" +
                    "  crs_arr_time INT,\n" +
                    "  carrier STRING,\n" +
                    "  flight_num INT,\n" +
                    "  tail_num INT,\n" +
                    "  act_el_time INT,\n" +
                    "  crs_el_time INT,\n" +
                    "  air_time INT,\n" +
                    "  arr_delay INT,\n" +
                    "  dep_delay INT,\n" +
                    "  origin STRING,\n" +
                    "  dest STRING,\n" +
                    "  dist INT,\n" +
                    "  taxi_in INT,\n" +
                    "  taxi_out INT,\n" +
                    "  cancelled INT,\n" +
                    "  cancellation_code STRING,\n" +
                    "  diverted INT,\n" +
                    "  carrier_dealy INT,\n" +
                    "  weather_delay INT,\n" +
                    "  nas_delay INT,\n" +
                    "  sec_delay INT,\n" +
                    "  late_aircraft_delay INT\n" +
                    ")\n" +
                    "%s\n" +
                    "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n" +
                    "WITH SERDEPROPERTIES \n" +
                    "(\n" +
                    "    \"separatorChar\" = \",\",\n" +
                    "    \"quoteChar\"     = \"\\\"\"\n" +
                    ")\n" +
                    "STORED AS TEXTFILE\n" +
                    "TBLPROPERTIES(\"skip.header.line.count\"=\"1\")",
            "CREATE TABLE flights%s (\n" +
                    "  year INT,\n" +
                    "  day_of_month INT,\n" +
                    "  day_of_week INT,\n" +
                    "  dep_time INT,\n" +
                    "  crs_dep_time INT,\n" +
                    "  arr_time INT,\n" +
                    "  crs_arr_time INT,\n" +
                    "  carrier STRING,\n" +
                    "  flight_num INT,\n" +
                    "  tail_num INT,\n" +
                    "  act_el_time INT,\n" +
                    "  crs_el_time INT,\n" +
                    "  air_time INT,\n" +
                    "  arr_delay INT,\n" +
                    "  dep_delay INT,\n" +
                    "  origin STRING,\n" +
                    "  dest STRING,\n" +
                    "  dist INT,\n" +
                    "  taxi_in INT,\n" +
                    "  taxi_out INT,\n" +
                    "  cancelled INT,\n" +
                    "  cancellation_code STRING,\n" +
                    "  diverted INT,\n" +
                    "  carrier_dealy INT,\n" +
                    "  weather_delay INT,\n" +
                    "  nas_delay INT,\n" +
                    "  sec_delay INT,\n" +
                    "  late_aircraft_delay INT\n" +
                    ")\n" +
                    "%s\n" +
                    "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n" +
                    "WITH SERDEPROPERTIES \n" +
                    "(\n" +
                    "    \"separatorChar\" = \",\",\n" +
                    "    \"quoteChar\"     = \"\\\"\"\n" +
                    ")\n" +
                    "STORED AS TEXTFILE\n" +
                    "TBLPROPERTIES(\"skip.header.line.count\"=\"1\")"
    ));

    public static final String createTableLike = "CREATE TABLE flights%s LIKE flights%s STORED AS %s";

    public static final String loadFlights = "LOAD DATA INPATH '/big-data-training/hive/lab4/2007.csv' OVERWRITE INTO TABLE flights0";

    public static final String loadFlightsFromTable = "INSERT INTO TABLE flights%s %s select year, day_of_month, " +
            "day_of_week, dep_time, crs_dep_time, arr_time, crs_arr_time, carrier, flight_num, tail_num, act_el_time, " +
            "crs_el_time, air_time, arr_delay, dep_delay, origin, dest, dist, taxi_in, taxi_out, cancelled, " +
            "cancellation_code, diverted, carrier_dealy, weather_delay, nas_delay, sec_delay, late_aircraft_delay, " +
            "month FROM flights0";

    public static final String createCarriers = "CREATE TABLE carriers (\n" +
            "  code STRING,\n" +
            "  desc STRING\n" +
            ")\n" +
            "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n" +
            "WITH SERDEPROPERTIES \n" +
            "(\n" +
            "    \"separatorChar\" = \",\",\n" +
            "    \"quoteChar\"     = \"\\\"\"\n" +
            ")\n" +
            "STORED AS TEXTFILE\n" +
            "TBLPROPERTIES(\"skip.header.line.count\"=\"1\")";

    public static final String loadCarriers = "LOAD DATA INPATH '/big-data-training/hive/lab4/carriers.csv' OVERWRITE INTO TABLE carriers";

    public static final String createAirports = "CREATE TABLE airports (\n" +
            "  iata STRING,\n" +
            "  airport STRING,\n" +
            "  city STRING,\n" +
            "  state STRING,\n" +
            "  country STRING,\n" +
            "  lat DOUBLE,\n" +
            "  lng DOUBLE\n" +
            ")\n" +
            "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n" +
            "WITH SERDEPROPERTIES \n" +
            "(\n" +
            "    \"separatorChar\" = \",\",\n" +
            "    \"quoteChar\"     = \"\\\"\"\n" +
            ")\n" +
            "STORED AS TEXTFILE\n" +
            "TBLPROPERTIES(\"skip.header.line.count\"=\"1\")";

    public static final String loadAirports = "LOAD DATA INPATH '/big-data-training/hive/lab4/airports.csv' OVERWRITE INTO TABLE airports";

    public static final String query1 = "SELECT c.desc AS carrier, COUNT(*) AS flights\n" +
            "FROM carriers AS c \n" +
            "JOIN flights%1$s AS f\n" +
            "ON c.code = f.carrier\n" +
            "GROUP BY c.desc\n" +
            "ORDER BY flights DESC";

    public static final String query2 = "SELECT SUM(sub.cnt) AS nyc_total\n" +
            "FROM (\n" +
            "\tSELECT COUNT(*) AS cnt \n" +
            "\tFROM airports AS a\n" +
            "\tJOIN flights%1$s AS f\n" +
            "\tON a.iata = f.origin\n" +
            "\tWHERE a.city = \"New York\" AND f.month = 6\n" +
            "\tUNION\n" +
            "\tSELECT COUNT(*) AS cnt\n" +
            "\tFROM airports AS a\n" +
            "\tJOIN flights%1$s AS f\n" +
            "\tON a.iata = f.dest\n" +
            "\tWHERE a.city = \"New York\" AND f.month = 6\n" +
            ") AS sub";

    public static final String query3 = "SELECT sub.airport AS airport, SUM(sub.cnt) AS flights\n" +
            "FROM (\n" +
            "\tSELECT a.airport AS airport, COUNT(*) AS cnt\n" +
            "\tFROM airports AS a\n" +
            "\tJOIN flights%1$s AS f\n" +
            "\tON a.iata = f.origin\n" +
            "\tWHERE a.country = \"USA\" AND f.month IN (6, 7, 8)\n" +
            "\tGROUP BY a.airport\n" +
            "\tUNION\n" +
            "\tSELECT a.airport AS airport, COUNT(*) AS cnt\n" +
            "\tFROM airports AS a\n" +
            "\tJOIN flights%1$s AS f\n" +
            "\tON a.iata = f.dest\n" +
            "\tWHERE a.country = \"USA\" AND f.month IN (6, 7, 8)\n" +
            "\tGROUP BY a.airport\n" +
            ") AS sub\n" +
            "GROUP BY airport\n" +
            "SORT BY flights DESC\n" +
            "LIMIT 5";

    public static final String query4 = "SELECT c.desc AS carrier, COUNT(*) AS flights\n" +
            "FROM carriers AS c\n" +
            "JOIN flights%1$s AS f\n" +
            "ON c.code = f.carrier\n" +
            "GROUP BY c.desc\n" +
            "SORT BY flights DESC\n" +
            "LIMIT 1";

    public static final String setMR = "set hive.execution.engine=mr";

    public static final String setTez = "set hive.execution.engine=tez";

    public static final String setVectorized = "set hive.vectorized.execution.enabled = true";

    public static final String setVectorizedReduce = "set hive.vectorized.execution.reduce.enabled = true";

    public static final String createIndex = "CREATE INDEX flights%1$s_dest_idx ON TABLE flights%1$s(dest)\n" +
            "AS 'COMPACT' WITH DEFERRED REBUILD";

    public static final String rebuildIndex = "ALTER INDEX flights%1$s_dest_idx ON flights%1$s REBUILD";

    public static final String dropIndex = "DROP INDEX flights%1$s_dest_idx ON flights%1$s";
}
