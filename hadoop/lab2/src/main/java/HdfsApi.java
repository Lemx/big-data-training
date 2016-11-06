import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HdfsApi {

    public static void main(String[] args)
            throws IOException, ExecutionException, InterruptedException {

        if (args.length < 3) {
            System.out.println("Usage: <hdfs-root-uri> <in-dir> <out-file>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", args[0]);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        Path inputDir = new Path(args[1]);
        Path outputFile = new Path(args[2]);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] statuses = fs.listStatus(inputDir);

        ExecutorService es = Executors.newFixedThreadPool(statuses.length);

        ArrayList<Future<Stream<Map.Entry<String, Integer>>>> tasks = new ArrayList<>();

        for (FileStatus status : statuses) {
            Future<Stream<Map.Entry<String, Integer>>> task = es.submit(new FileParser(status.getPath(), conf));
            tasks.add(task);
        }

        Stream<Map.Entry<String, Integer>> result = Stream.empty();

        for (Future<Stream<Map.Entry<String, Integer>>> task : tasks) {
            result = Stream.concat(result, task.get());
        }

        List<Map.Entry<String, Integer>> list = result
                                                .collect(Collectors.toMap(
                                                        e -> e.getKey(),
                                                        e -> e.getValue(),
                                                        (v1, v2) -> v1 + v2))
                                                .entrySet()
                                                .stream()
                                                .sorted((e1, e2) -> -e1.getValue().compareTo(e2.getValue()))
                                                .limit(100)
                                                .collect(Collectors.toList());
        es.shutdown();

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

