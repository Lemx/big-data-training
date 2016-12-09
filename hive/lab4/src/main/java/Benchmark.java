import au.com.bytecode.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Benchmark {

    private static ArrayList<String> fileFormats = new ArrayList<>(Arrays.asList(
            "TEXTFILE",
            "AVRO",
            "RCFILE",
            "ORC",
            "PARQUET"
    ));

    private static ArrayList<String> partitions = new ArrayList<>(Arrays.asList(
            "",
            "PARTITIONED BY (month INT)",
            "CLUSTERED BY(origin) INTO 32 BUCKETS",
            "CLUSTERED BY(carrier) INTO 32 BUCKETS",
            "PARTITIONED BY (month INT)\n CLUSTERED BY(carrier) INTO 32 BUCKETS"
    ));

    private static ArrayList<String> partitionsForInsert = new ArrayList<>(Arrays.asList(
            "",
            "PARTITION (month)",
            "",
            "",
            "PARTITION (month)"
    ));

    public static void main(String[] args) throws SQLException, IOException {
        prepareFiles();
        prepareTables();
        List<ReportEntry> report = runExperiment();
        writeReport(report);
    }

    private static void writeReport(List<ReportEntry> report)
            throws IOException {
        CSVWriter writer = new CSVWriter(new FileWriter("report.csv"), ',');

        for (ReportEntry reportEntry : report) {
            String[] entries = new String[] { reportEntry.getTable(),
                                                reportEntry.getSize().toString(),
                                                reportEntry.getQueryNumber().toString(),
                                                reportEntry.getExecutionTime().toString()};
            writer.writeNext(entries);
        }

        writer.close();
    }

    private static void prepareFiles()
            throws IOException {
        FileHelper.copyToHdfs("/Users/anatoliyfetisov/Projects/big-data-training/hive/airports.csv",
                "/big-data-training/hive/lab4/airports.csv");
        FileHelper.copyToHdfs("/Users/anatoliyfetisov/Projects/big-data-training/hive/carriers.csv",
                "/big-data-training/hive/lab4/carriers.csv");
        FileHelper.copyToHdfs("/Users/anatoliyfetisov/Projects/big-data-training/hive/2007.csv",
                "/big-data-training/hive/lab4/2007.csv");

    }

    private static void prepareTables()
            throws SQLException {
        HiveClient hiveClient = new HiveClient("jdbc:hive2://localhost:10000", "default", "hive", "hive");
        hiveClient.executeQuery("CREATE DATABASE IF NOT EXISTS lab4");

        for (Integer i = 0; i < Queries.createFlights.size() * fileFormats.size(); i++) {

            hiveClient = new HiveClient("jdbc:hive2://localhost:10000", "lab4", "hive", "hive");

            String createTable = Queries.createFlights.get(i / Queries.createFlights.size());
            String fileFormat = fileFormats.get(i % fileFormats.size());

            if (i == 0) {
                hiveClient.executeQuery(String.format(createTable, i, partitions.get(i / 5)));
                hiveClient.executeQuery(Queries.loadFlights);
            } else {
                if (i % 5 == 0) {
                    hiveClient.executeQuery(String.format(createTable, i, partitions.get(i / 5)));
                } else {
                    hiveClient.executeQuery(String.format(Queries.createTableLike, i, (5 * (i/5)), fileFormat));
                }
                hiveClient.executeQuery(String.format(Queries.loadFlightsFromTable, i, partitionsForInsert.get(i / 5)));
            }
        }

        hiveClient = new HiveClient("jdbc:hive2://localhost:10000", "lab4", "hive", "hive");
        hiveClient.executeQuery(Queries.createCarriers);
        hiveClient.executeQuery(Queries.loadCarriers);
        hiveClient.executeQuery(Queries.createAirports);
        hiveClient.executeQuery(Queries.loadAirports);
    }

    private static List<ReportEntry> runExperiment()
            throws SQLException {
        List<ReportEntry> report = new ArrayList<>();

        for (Integer i = 0; i < Queries.createFlights.size() * fileFormats.size(); i++) {
            HiveClient hiveClient = new HiveClient("jdbc:hive2://localhost:10000", "lab4", "hive", "hive");
            String table = "flights" + i;
            Long size = hiveClient.getTableSize(table);

            hiveClient.executeQuery(Queries.setMR);
            RunQueries(report, i, hiveClient, table, size);

            hiveClient.executeQuery(String.format(Queries.createIndex, i));
            hiveClient.executeQuery(String.format(Queries.rebuildIndex, i));
            RunQueries(report, i, hiveClient, table, size);

            hiveClient.executeQuery(Queries.setTez);
            RunQueries(report, i, hiveClient, table, size);

            hiveClient.executeQuery(Queries.setMR);
            hiveClient.executeQuery(Queries.setVectorized);
            hiveClient.executeQuery(Queries.setVectorizedReduce);
            RunQueries(report, i, hiveClient, table, size);
        }

        return report;
    }

    private static void RunQueries(List<ReportEntry> report, Integer tableNumber,
                                   HiveClient hiveClient, String table, Long size) {
        Long time;

        try {
            time = runAndMeasureQuery(Queries.query1, hiveClient, tableNumber);
            report.add(new ReportEntry(table, size, 1, time));
        } catch (SQLException e) {
            report.add(new ReportEntry("**FAILED** " + table, size, 1, -1l));
        }

        try {
            time = runAndMeasureQuery(Queries.query2, hiveClient, tableNumber);
            report.add(new ReportEntry(table, size, 2, time));
        } catch (SQLException e) {
            report.add(new ReportEntry("**FAILED** " + table, size, 2, -1l));
        }

        try {
            time = runAndMeasureQuery(Queries.query3, hiveClient, tableNumber);
            report.add(new ReportEntry(table, size, 3, time));
        } catch (SQLException e) {
            report.add(new ReportEntry("**FAILED** " + table, size, 3, -1l));
        }

        try {
            time = runAndMeasureQuery(Queries.query4, hiveClient, tableNumber);
            report.add(new ReportEntry(table, size, 4, time));
        } catch (SQLException e) {
            report.add(new ReportEntry("**FAILED** " + table, size, 4, -1l));
        }
    }

    private static Long runAndMeasureQuery(String query, HiveClient hiveClient, Integer tableNumber)
            throws SQLException {
        Long startTime = System.currentTimeMillis();
        hiveClient.executeQuery(String.format(query, tableNumber));
        Long endTime = System.currentTimeMillis();
        return (endTime - startTime) / 1000;
    }
}
