import java.sql.*;

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

    public Long getTableSize(String tableName)
            throws SQLException {

        Long size = -1l;

        if (connect()) {
            Statement stmt = connection.createStatement();
            ResultSet res = stmt.executeQuery("show tblproperties " + tableName + "('totalSize')");
            while (res.next()) {
                size = res.getLong("prpt_name");
            }
        }

        return size;
    }
}
