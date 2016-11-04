import com.google.common.base.Functions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileParser implements Callable<Stream<Map.Entry<String, Integer>>> {

    private final Path path;
    private final Configuration configuration;

    public FileParser(Path path, Configuration conf) {
        this.path = path;
        configuration = conf;
    }

    @Override
    public Stream<Map.Entry<String, Integer>> call()
            throws Exception {
        HashMap<String, Integer> entries = new HashMap<>();
        BufferedReader br = new BufferedReader(new InputStreamReader(FileSystem.get(configuration).open(path)));
        String line = br.readLine();
        while (line != null) {
            String id = getId(line);
            if (id.equals("null")) { // excluding lines with "null"-value id
                line = br.readLine();
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

        return entries
                .entrySet()
                .stream();
    }

    private static String getId(String entry) {
        return entry.split("\t")[2];
    }
}
