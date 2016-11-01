import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinPartitioningReducer extends Reducer<Text, Text, Text, IntWritable> {

    private IntWritable outputValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Integer sum = 0;
        for (Text value : values) {
            sum += 1;
        }

        outputValue.set(sum);
        context.write(key, outputValue);
    }
}
