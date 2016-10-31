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
            IntWritable key = null;
            Iterable<Text> values = null;
            while(context.nextKey()) {
                key = context.getCurrentKey();
                values = context.getValues();
            }
            this.reduce(key, values, context);
            Iterator iter = values.iterator();
            if(iter instanceof ReduceContext.ValueIterator) {
                ((ReduceContext.ValueIterator)iter).resetBackupStore();
            }
        } finally {
            this.cleanup(context);
        }
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, values.iterator().next()); // writing first longest word only
    }
}