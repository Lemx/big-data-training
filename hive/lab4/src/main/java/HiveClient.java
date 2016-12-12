import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class HiveClient {

    private final static String driverClassName = "org.apache.hive.jdbc.HiveDriver";

    private final String connectionString;
    private Connection connection;
    private Boolean initialized = false;


    public HiveClient(String address, String dbName, String user, String password) {
        connectionString = address + "/" + dbName + ";user=" + user + ";password=" + password + ";";
    }

    public Boolean connect() {
        if (initialized)
            return true;

        try {
            Class.forName(driverClassName);
            connection = DriverManager.getConnection(connectionString);
            initialized = true;
        }
        catch (ClassNotFoundException e) {
            return false;
        }
        catch (SQLException e) {
            return false;
        }
        return true;
    }

    public void executeQuery(String query) throws SQLException {
        if (connect()) {
            Statement stmt = connection.createStatement();
            stmt.execute(query);
        }
    }


    // there is no reliable way to query Hive for partitioned table size, so we use HDFS API
    public Long getTableSize(String table)
            throws SQLException, IOException {
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS","hdfs://localhost:8020");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fSystem = FileSystem.get(conf);

        ContentSummary summary = fSystem.getContentSummary(new Path("/apps/hive/warehouse/lab4.db/" + table));
        return (summary.getSpaceConsumed() / Integer.parseInt(conf.get("dfs.replication"))) / (1024 * 1024);
    }
}
