import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class JoinPartitioningMapper extends Mapper<Object, Text, Text, Text> {

    private static final HashMap<String, String> Cities = new HashMap<String, String>();


    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        URI[] cacheFilesLocal = context.getCacheFiles();
        for (URI path : cacheFilesLocal) {
            if (path.toString().trim().endsWith("city.en.txt")) {
                loadCities(path, context);
            }
        }
    }

    private void loadCities(URI path, Context context)
            throws IOException {
        String line;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(new Path(path))));
            line = br.readLine();
            while (line != null) {
                String[] splittedLine = line.split("\\s");
                String id = splittedLine[0].trim();
                String city = splittedLine[1].trim();
                Cities.put(id, city);
                line = br.readLine();
            }
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String userAgent;
        String cityId;
        String bidString;

        String line = value.toString();
        String[] splittedLine = line.split("\\t");

        bidString = splittedLine[19];
        if (tryParseInt(bidString)) { // skipping low-bid entries
            Integer bid = Integer.parseInt(bidString);
            if (bid > 250) {
                userAgent = splittedLine[4];
                cityId = splittedLine[7];
                String city = Cities.get(cityId);
                if (city == null) {
                    city = Cities.get("0"); // unknown city
                }
                outputKey.set(city);
                outputValue.set(userAgent);
                context.write(outputKey, outputValue);
            }
        }
    }


    private Boolean tryParseInt(String value) {
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
