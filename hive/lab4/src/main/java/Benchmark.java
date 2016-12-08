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

    private static final ArrayList<ReportEntry> report = new ArrayList<>();


    public static void main(String[] args) throws SQLException, IOException {
//        prepareFiles();
        prepareTables();
        List<ReportEntry> report = runExperiment();
        writeReport(report);
    }

    private static void writeReport(List<ReportEntry> report)
            throws IOException {
        CSVWriter writer = new CSVWriter(new FileWriter("report.csv"), '\t');

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

        for (Integer i = 15; i < Queries.createFlights.size() * fileFormats.size(); i++) {

            hiveClient = new HiveClient("jdbc:hive2://localhost:10000", "lab4", "hive", "hive");
//            hiveClient.executeQuery(Queries.setDynamicPart);
            String createTable = Queries.createFlights.get(i / Queries.createFlights.size());
            String fileFormat = fileFormats.get(i % fileFormats.size());

            if (i == 0) {
                hiveClient.executeQuery(String.format(createTable, i, partitions.get(i / 5)));
                hiveClient.executeQuery(String.format(Queries.loadFlights, i, i));
            } else {
                if (i % 5 == 0) {
                    hiveClient.executeQuery(String.format(createTable, i, partitions.get(i / 5)));
                } else {
                    hiveClient.executeQuery(String.format(Queries.createTableLike, i, partitions.get(i / 5), fileFormat));
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

        for (Integer i = 1; i < 25; i++) {
            HiveClient hiveClient = new HiveClient("jdbc:hive2://localhost:10000", "lab4", "hive", "hive");
            String table = "flights" + i;
            Long size = hiveClient.getTableSize(table);

            hiveClient.executeQuery(Queries.setMR);
            RunQueries(report, i, hiveClient, table, size);

            hiveClient.executeQuery(Queries.createIndex);
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

    private static void RunQueries(List<ReportEntry> report, Integer i, HiveClient hiveClient, String table, Long size)
            throws SQLException {
        Long time = runAndMeasureQuery(Queries.query1, hiveClient, i);
        report.add(new ReportEntry(table, size, 1, time));
        time = runAndMeasureQuery(Queries.query1, hiveClient, i);
        report.add(new ReportEntry(table, size, 2, time));
        time = runAndMeasureQuery(Queries.query1, hiveClient, i);
        report.add(new ReportEntry(table, size, 3, time));
        time = runAndMeasureQuery(Queries.query1, hiveClient, i);
        report.add(new ReportEntry(table, size, 4, time));
    }

    private static Long runAndMeasureQuery(String query, HiveClient hiveClient, Integer tableNumber)
            throws SQLException {
        Long startTime = System.currentTimeMillis();
        hiveClient.executeQuery(String.format(Queries.query1, tableNumber));
        Long endTime = System.currentTimeMillis();
        return (endTime - startTime) / 1000;
    }
}
