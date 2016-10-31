import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LongestWordMapper
        extends Mapper<Object, Text, IntWritable, Text> {

    private IntWritable length = new IntWritable();
    private Text word = new Text();
    private static final Pattern pattern = Pattern.compile("\\w+");

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        Matcher matcher = pattern.matcher(value.toString());
        Integer maxLen = -1;
        String longestWord = "";
        while (matcher.find()) {
            String localWord = matcher.group().trim().toLowerCase();
            Integer len = localWord.length();
            if (len > maxLen) {
                longestWord = localWord;
                maxLen = len;
            }
        }

        if (maxLen > 0) {
            word.set(longestWord);
            length.set(maxLen);
            context.write(length, word);
        }
    }
}

