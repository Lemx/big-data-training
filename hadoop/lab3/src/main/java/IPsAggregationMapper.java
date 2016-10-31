import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class IPsAggregationMapper extends Mapper<Object, Text, Text, LongWritable> {

    private Text ip = new Text();
    private LongWritable bytes = new LongWritable();

    private static final String regex = "^(\\w+)\\s[\\w.-]+\\s[\\w.-]+\\s\\[.*\\]\\s\".*\"\\s\\d{3}\\s(\\d+|-)\\s\".*\"\\s\"(.*)\"$";
    private static final Pattern pattern = Pattern.compile(regex);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String ipString;
        Long bytesCount;

        Matcher matcher = pattern.matcher(value.toString());
        if (matcher.matches()) {
            ipString = matcher.group(1);
            String bytesString = matcher.group(2);
            if (!bytesString.equals("-")) { // skipping entries with no bytes
                Browsers.KIND browser = getBrowser(matcher.group(3).toLowerCase());
                context.getCounter(browser).increment(1);
                bytesCount = Long.parseLong(bytesString);
                ip.set(ipString);
                this.bytes.set(bytesCount);
                context.write(ip, bytes);
            }
        }
    }

    private Browsers.KIND getBrowser(String agentInfo) {
        if (agentInfo.contains("msie")) {
            return Browsers.KIND.IE;
        }
        else if (agentInfo.contains("mozilla")){
            return Browsers.KIND.MOZILLA;
        }
        else {
            return Browsers.KIND.OTHER;
        }
    }
}
