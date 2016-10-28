import static java.lang.System.out;

import org.apache.commons.collections.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.HostsFileReader;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class HdfsApi {

    private static final HashMap<String, Integer> entries = new HashMap<String, Integer>();

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000/");

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] statuses = fs.listStatus(new Path(args[0]));

        for (FileStatus status : statuses) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
            String id = br.readLine();
            while (id != null) {
                if (entries.containsKey(id)) {
                    Integer currentCount = entries.get(id);
                    entries.put(id, currentCount + 1);
                } else {
                    entries.put(id, 1);
                }
                id = getId(br.readLine());
            }
            br.close();
        }

        fs.createNewFile(new Path(args[1]));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.append(new Path(args[1]))));

        //TODO: sort hashmap

        for (Map.Entry<String, Integer> entry : entries.entrySet()) {
            bw.write(entry.getKey() + "\t" + entry.getValue());
        }
        bw.close();
    }

    private static String getId(String entry) {
        return entry.split("\t")[2];
    }
}

