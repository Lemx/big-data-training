import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class JoinPartitioningPartitioner extends Partitioner<Text,Text> {

    public int getPartition(Text key, Text value, int numReduceTasks){
        if(numReduceTasks==0)
            return 0;
        if(value.toString().toLowerCase().contains("windows")) {
            return 1 % numReduceTasks;
        } else {
          return 2 % numReduceTasks;
        }

    }
}