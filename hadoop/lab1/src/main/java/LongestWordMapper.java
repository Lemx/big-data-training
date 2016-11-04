import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LongestWordMapper
        extends Mapper<Object, Text, IntWritable, Text> {

    private IntWritable outputKey = new IntWritable();
    private Text outputValue = new Text();

    private Integer maxLen = -1;
    private String longestWord = "";

    private static final Pattern pattern = Pattern.compile("\\w+");

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        Matcher matcher = pattern.matcher(value.toString());
        while (matcher.find()) {
            String localWord = matcher.group().trim().toLowerCase();
            Integer len = localWord.length();
            if (len > maxLen) {
                longestWord = localWord;
                maxLen = len;
            }
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        outputKey.set(maxLen);
        outputValue.set(longestWord);
        context.write(outputKey, outputValue);
    }
}

