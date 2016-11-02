import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class LongestWordReducer
    extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void run(Context context)
            throws IOException, InterruptedException {
        this.setup(context);

        try {
            context.nextKey();
            IntWritable key = context.getCurrentKey();
            Text value = context.getCurrentValue();
            context.write(key, value);
        } finally {
            this.cleanup(context);
        }
    }
}