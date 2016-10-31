import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IPsAggregationReducer extends Reducer<Text, LongWritable, Text, FloatLongWritablePair> {

    private FloatLongWritablePair floatLongWritablePair = new FloatLongWritablePair();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        Integer count = 0;
        Long sum = 0l;
        for (LongWritable val : values) {
            count++;
            sum += val.get();
        }

        floatLongWritablePair.set(sum / count.floatValue(), sum);

        context.write(key, floatLongWritablePair);
    }
}
