import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class LongestWordReducer
    extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Text t = new Text("");
        Integer maxLen = -1;
        for (Text val : values) {
            String localWord = val.toString();
            Integer len = localWord.length();
            if (len > maxLen) {
                t.set(localWord);
                maxLen = len;
            }
        }

        context.write(new IntWritable(maxLen), t);
    }
}