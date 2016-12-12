import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class FileHelper {

    public static void copyToHdfs(String from, String to)
            throws IOException {
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS","hdfs://localhost:8020");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fSystem = FileSystem.get(conf);

        fSystem.copyFromLocalFile(false, true, new Path(from), new Path(to));
    }

    // there is no reliable way to query Hive for partitioned table size, so we use HDFS API
    public static Long getTableSize(String table)
            throws IOException {
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS","hdfs://localhost:8020");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fSystem = FileSystem.get(conf);

        ContentSummary summary = fSystem.getContentSummary(new Path("/apps/hive/warehouse/lab4.db/" + table));
        return summary.getLength() / (1024 * 1024);
    }
}
