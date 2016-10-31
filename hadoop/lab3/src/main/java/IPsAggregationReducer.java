import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IPsAggregationReducer extends Reducer<Text, LongWritable, Text, Text> {

    private Text text = new Text();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        Integer count = 0;
        Long sum = 0l;
        for (LongWritable val : values) {
            count++;
            sum += val.get();
        }

        text.set(String.format("%1$.2f, %2$d", sum / count.floatValue(), sum));
        context.write(key, text);
    }
}
