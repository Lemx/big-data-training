import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JoinPartitioningMapper extends Mapper<Object, Text, Text, Text> {

    private static final String regex = "^\\w+\\t\\w+\\t\\w\\t\\w*\\t(.*\\))\\t[\\w\\.\\*]+\\t\\w+\\t(\\w+)\\t\\w+\\t\\w+\\t\\w+\\t\\w+\\t\\w+\\t\\w+\\t\\w+\\t\\w+\\t\\w+\\t\\w+\\t\\w+\\t(\\w+)\\t\\w+\\t\\w+\\t\\w+\\t.*$";
    private static final Pattern pattern = Pattern.compile(regex);
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
                if (city.contains("_")) {
                    String[] splittedCity = city.split("_");
                    String extraId = splittedCity[1];
                    if (tryParseInt(extraId)) {
                        city = splittedCity[0];
                        Cities.put(extraId, city);
                    }
                }
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
        Matcher matcher = pattern.matcher(line);
        if (matcher.matches()) {
            bidString = matcher.group(3);
            if (tryParseInt(bidString)) { // skipping low-bid entries
                Integer bid = Integer.parseInt(bidString);
                if (bid > 250) {
                    userAgent = matcher.group(1);
                    cityId = matcher.group(2);
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
