import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;

public class PiCalc {

    public static void main(String[] args)
            throws IOException {

        final Integer iters = Integer.parseInt(args[0]);
        final Integer len = Integer.parseInt(args[1]);
        final String fsUri = args[2];
        final String contNumber = args[3];

        BigDecimal num2power6 = new BigDecimal(64);
        BigDecimal sum = new BigDecimal(0);
        for(int i = 0; i < iters; i++ ) {
            BigDecimal tmp;
            BigDecimal term ;
            BigDecimal divisor;
            term = new BigDecimal(-32);
            divisor = new BigDecimal(4*i+1);
            tmp =  term.divide(divisor, len, BigDecimal.ROUND_FLOOR);
            term = new BigDecimal(-1);
            divisor = new BigDecimal(4*i+3);
            tmp = tmp.add(term.divide(divisor, len, BigDecimal.ROUND_FLOOR));
            term = new BigDecimal(256);
            divisor = new BigDecimal(10*i+1);
            tmp = tmp.add(term.divide(divisor, len, BigDecimal.ROUND_FLOOR));
            term = new BigDecimal(-64);
            divisor = new BigDecimal(10*i+3);
            tmp = tmp.add(term.divide(divisor, len, BigDecimal.ROUND_FLOOR));
            term = new BigDecimal(-4);
            divisor = new BigDecimal(10*i+5);
            tmp = tmp.add(term.divide(divisor, len, BigDecimal.ROUND_FLOOR));
            term = new BigDecimal(-4);
            divisor = new BigDecimal(10*i+7);
            tmp = tmp.add(term.divide(divisor, len, BigDecimal.ROUND_FLOOR));
            term = new BigDecimal(1);
            divisor = new BigDecimal(10*i+9);
            tmp = tmp.add(term.divide(divisor, len, BigDecimal.ROUND_FLOOR));
            int s = ((1-((i&1)<<1)));
            divisor = new BigDecimal(2);
            divisor = divisor.pow(10*i).multiply(new BigDecimal(s));
            sum = sum.add(tmp.divide(divisor, len, BigDecimal.ROUND_FLOOR));

            if (i % 100 == 0) {
                System.out.println("Iteration #" + i + " done");
            }
        }
        sum = sum.divide(num2power6, len, BigDecimal.ROUND_FLOOR);
        String result = sum.toPlainString();

        System.out.println("Finished, trying to write result to file.");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", fsUri);
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(configuration);

        Path outputFile = new Path("/big-data-training/hadoop/lab5/output/result_" + contNumber + ".txt");

        try {
            if (fs.exists(outputFile)) {
                fs.delete(outputFile, true);
            }

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(outputFile)));
            bw.write(sum.toPlainString());
            bw.close();
        }
        catch (Throwable throwable)
        {
            System.out.println(throwable.toString());
        }


    }
}
