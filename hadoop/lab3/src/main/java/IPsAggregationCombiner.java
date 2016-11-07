import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IPsAggregationCombiner extends Reducer<Text, IntLongWritablePair, Text, IntLongWritablePair> {

    private final IntLongWritablePair outputValue = new IntLongWritablePair();

    @Override
    protected void reduce(Text key, Iterable<IntLongWritablePair> values, Context context)
            throws IOException, InterruptedException {

        Integer count = 0;
        Long sum = 0l;
        for (IntLongWritablePair val : values) {
            count += val.getInt().get();
            sum += val.getLong().get();
        }

        outputValue.set(count, sum);

        context.write(key, outputValue);
    }
}
