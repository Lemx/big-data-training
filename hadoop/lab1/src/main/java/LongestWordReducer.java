import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class LongestWordReducer
    extends Reducer<IntWritable, Text, IntWritable, Text> {

    private static final IntWritable constantKey = new IntWritable(0);
    private Text value = new Text();

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Integer maxLen = -1;
        for (Text val : values) {
            String localWord = val.toString();
            Integer len = localWord.length();
            if (len > maxLen) {
                value.set(localWord);
                maxLen = len;
            }
        }

        context.write(constantKey, value);
    }
}