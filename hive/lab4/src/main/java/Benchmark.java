import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

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

    private static Map<String, String> tables = new HashMap<String, String>() {{
        put("flights0", "NAIVE SCHEMA/TEXTFILE");
        put("flights1", "NAIVE SCHEMA/AVRO");
        put("flights2", "NAIVE SCHEMA/RCFILE");
        put("flights3", "NAIVE SCHEMA/ORC");
        put("flights4", "NAIVE SCHEMA/PARQUET");
        put("flights5", "PARTITIONED BY month/TEXTFILE");
        put("flights6", "PARTITIONED BY month/AVRO");
        put("flights7", "PARTITIONED BY month/RCFILE");
        put("flights8", "PARTITIONED BY month/ORC");
        put("flights9", "PARTITIONED BY month/PARQUET");
        put("flights10", "CLUSTERED BY origin/TEXTFILE");
        put("flights11", "CLUSTERED BY origin/AVRO");
        put("flights12", "CLUSTERED BY origin/RCFILE");
        put("flights13", "CLUSTERED BY origin/ORC");
        put("flights14", "CLUSTERED BY origin/PARQUET");
        put("flights15", "CLUSTERED BY carrier/TEXTFILE");
        put("flights16", "CLUSTERED BY carrier/AVRO");
        put("flights17", "CLUSTERED BY carrier/RCFILE");
        put("flights18", "CLUSTERED BY carrier/ORC");
        put("flights19", "CLUSTERED BY carrier/PARQUET");
        put("flights20", "PARTITIONED BY month CLUSTERED BY carrier/TEXTFILE");
        put("flights21", "PARTITIONED BY month CLUSTERED BY carrier/AVRO");
        put("flights22", "PARTITIONED BY month CLUSTERED BY carrier/RCFILE");
        put("flights23", "PARTITIONED BY month CLUSTERED BY carrier/ORC");
        put("flights24", "PARTITIONED BY month CLUSTERED BY carrier/PARQUET");
    }};

    public static void main(String[] args)
            throws SQLException, IOException {
        prepareFiles();
        prepareTables();
        prepareReport();
        runExperiment();
    }

    private static void prepareReport()
            throws IOException {
        if (new File("report.csv").exists())
            return;

        CSVWriter writer = new CSVWriter(new FileWriter("report.csv"), ',', CSVWriter.NO_QUOTE_CHARACTER);
        writer.writeNext(new String[] {"table", "format", "size (MB)", "query", "time (seconds)", "comment"});
        writer.close();
    }

    private static void appendReport(List<ReportEntry> report)
            throws IOException {
        CSVWriter writer = new CSVWriter(new FileWriter("report.csv", true), ',', CSVWriter.NO_QUOTE_CHARACTER);

        for (ReportEntry reportEntry : report) {
            String[] tableAndFormat =  tables.get(reportEntry.getTable()).split("/");
            String[] entries = new String[] { tableAndFormat[0],
                                                tableAndFormat[1],
                                                reportEntry.getSize().toString(),
                                                reportEntry.getQueryNumber().toString(),
                                                reportEntry.getExecutionTime().toString(),
                                                reportEntry.getComment()};
            writer.writeNext(entries);
        }

        writer.close();
    }

    private static void prepareFiles()
            throws IOException {
        FileFilter fileFilter = new WildcardFileFilter("*.csv");
        for (File file : new File(Paths.get(".").toAbsolutePath().normalize().toString()).listFiles(fileFilter)) {
            FileHelper.copyToHdfs(file.getAbsolutePath(), "/big-data-training/hive/lab4/" + file.getName());;
        }
    }

    private static void prepareTables()
            throws SQLException {
        HiveClient hiveClient = new HiveClient("jdbc:hive2://localhost:10000", "default", "hive", "hive");
        hiveClient.executeQuery("CREATE DATABASE IF NOT EXISTS lab4");
        hiveClient.close();

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
            hiveClient.close();
        }

        hiveClient = new HiveClient("jdbc:hive2://localhost:10000", "lab4", "hive", "hive");
        hiveClient.executeQuery(Queries.createCarriers);
        hiveClient.executeQuery(Queries.loadCarriers);
        hiveClient.executeQuery(Queries.createAirports);
        hiveClient.executeQuery(Queries.loadAirports);
        hiveClient.close();
    }

    private static void runExperiment()
            throws SQLException, IOException {

        for (Integer i = 21; i < Queries.createFlights.size() * fileFormats.size(); i++) {
            List<ReportEntry> report = new ArrayList<>();
            HiveClient hiveClient = new HiveClient("jdbc:hive2://localhost:10000", "lab4", "hive", "hive");
            String table = "flights" + i;
            Long size = FileHelper.getTableSize(table);

            hiveClient.executeQuery(Queries.setMR);
            RunQueries(report, i, hiveClient, table, size, "MR DEFAULT");

            // sometimes index build fails with no apparent reason
            try {
                hiveClient.executeQuery(String.format(Queries.createIndex, i));
                hiveClient.executeQuery(String.format(Queries.rebuildIndex, i));
                RunQueries(report, i, hiveClient, table, size, "MR WITH INDEX");

            } catch (SQLException e) {
                for (int j = 1; j < 5; j++) {
                    report.add(new ReportEntry(table, size, j, -1L, "FAILED INDEX BUILD"));
                }
            } finally {
                hiveClient.executeQuery(String.format(Queries.dropIndex, i));
            }

            hiveClient.executeQuery(Queries.setTez);
            RunQueries(report, i, hiveClient, table, size, "TEZ");

            hiveClient.executeQuery(Queries.setMR);
            hiveClient.executeQuery(Queries.setVectorized);
            hiveClient.executeQuery(Queries.setVectorizedReduce);
            RunQueries(report, i, hiveClient, table, size, "MR WITH VECTORIZATION");
            hiveClient.close();
            appendReport(report);
        }
    }

    private static void RunQueries(List<ReportEntry> report, Integer tableNumber,
                                   HiveClient hiveClient, String table, Long size, String comment) {
        Long time;

        try {
            time = runAndMeasureQuery(Queries.query1, hiveClient, tableNumber);
            report.add(new ReportEntry(table, size, 1, time, comment));
        } catch (SQLException e) {
            report.add(new ReportEntry(table, size, 1, -1L, "FAILED " + comment));
        }

        try {
            time = runAndMeasureQuery(Queries.query2, hiveClient, tableNumber);
            report.add(new ReportEntry(table, size, 2, time, comment));
        } catch (SQLException e) {
            report.add(new ReportEntry(table, size, 2, -1L, "FAILED " + comment));
        }

        try {
            time = runAndMeasureQuery(Queries.query3, hiveClient, tableNumber);
            report.add(new ReportEntry(table, size, 3, time, comment));
        } catch (SQLException e) {
            report.add(new ReportEntry(table, size, 3, -1L, "FAILED " + comment));
        }

        try {
            time = runAndMeasureQuery(Queries.query4, hiveClient, tableNumber);
            report.add(new ReportEntry(table, size, 4, time, comment));
        } catch (SQLException e) {
            report.add(new ReportEntry(table, size, 4, -1L, "FAILED " + comment));
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
