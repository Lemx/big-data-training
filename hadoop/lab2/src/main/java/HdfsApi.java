import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class HdfsApi {

    private static final HashMap<String, Integer> entries = new HashMap<>();

    public static void main(String[] args)
            throws IOException, ExecutionException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000/");

        Path inputDir = new Path(args[0]);
        Path outputFile = new Path(args[1]);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] statuses = fs.listStatus(inputDir);

        ExecutorService es = Executors.newFixedThreadPool(statuses.length);

        ArrayList<Future<Map<String, Integer>>> tasks = new ArrayList<>();

        for (FileStatus status : statuses) {
            Future<Map<String, Integer>> task = es.submit(new FileParser(status.getPath(), conf));
            tasks.add(task);
        }

        for (Future<Map<String, Integer>> task : tasks) {
            for (Map.Entry<String, Integer> entry : task.get().entrySet()) {
                entries.merge(entry.getKey(), entry.getValue(), (integer, integer2) -> integer + integer2);
            }
        }

        List<Map.Entry<String, Integer>> list = entries.entrySet()
                                                        .stream()
                                                        .sorted((k1, k2) -> -k1.getValue().compareTo(k2.getValue()))
                                                        .limit(100)
                                                        .collect(Collectors.toList());

        if (fs.exists(outputFile)) {
            fs.delete(outputFile, true);
        }

        fs.createNewFile(outputFile);
        fs.setReplication(outputFile, (short)1);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.append(outputFile)));

        for (Map.Entry<String, Integer> e : list) {
            bw.write(e.getKey() + "\t" + e.getValue() + "\n");
        }
        bw.close();

        System.exit(0);
    }
}

