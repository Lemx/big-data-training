import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HdfsApi {

    private static final HashMap<String, Integer> entries = new HashMap<>();

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000/");

        Path inputDir = new Path(args[0]);
        Path outputFile = new Path(args[1]);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] statuses = fs.listStatus(inputDir);

        for (FileStatus status : statuses) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
            String line = br.readLine();
            while (line != null) {
                String id = getId(line);
                if (id.equals("null")) { // excluding lines with "null"-value id
                    continue;
                }
                if (entries.containsKey(id)) {
                    Integer currentCount = entries.get(id);
                    entries.put(id, currentCount + 1);
                } else {
                    entries.put(id, 1);
                }
                line = br.readLine();
            }
            br.close();
        }

        List<String> list = entries.entrySet()
                                    .stream()
                                    .sorted((k1, k2) -> -k1.getValue().compareTo(k2.getValue()))
                                    .map(Map.Entry::getKey)
                                    .limit(100)
                                    .collect(Collectors.toList());

        if (fs.exists(outputFile)) {
            fs.delete(outputFile, true);
        }

        fs.createNewFile(outputFile);
        fs.setReplication(outputFile, (short)1);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.append(outputFile)));

        for (String id : list) {
            bw.write(id + "\n");
        }
        bw.close();
    }

    private static String getId(String entry) {
        return entry.split("\t")[2];
    }

}

