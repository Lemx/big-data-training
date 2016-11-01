import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JoinPartitioningDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new JoinPartitioningDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        conf.set("fs.defaultFS", "hdfs://localhost:9000/");

        Job job = Job.getInstance(conf);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Please, specify exactly three parameters: <in-dir> <to_cache-file> <out-dir>");
            System.exit(2);
        }

        job.setNumReduceTasks(4);

        job.setJobName("JoinPartitioning");
        job.setJarByClass(JoinPartitioningDriver.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(JoinPartitioningMapper.class);
        job.setReducerClass(JoinPartitioningReducer.class);
        job.setPartitionerClass(JoinPartitioningPartitioner.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus status = fs.getFileStatus(new Path(otherArgs[1]));
        job.addCacheFile(status.getPath().toUri());


        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        Boolean jobSuccessful =  job.waitForCompletion(true);

        return jobSuccessful ? 0 : 1;
    }
}
