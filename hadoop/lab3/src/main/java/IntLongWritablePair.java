import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntLongWritablePair implements Writable {

    private IntWritable intg = new IntWritable(0);
    private LongWritable lng = new LongWritable(0l);

    public IntLongWritablePair() {

    }

    public IntLongWritablePair(Integer intg, Long lng) {
        this.intg.set(intg);
        this.lng.set(lng);
    }

    public IntWritable getInt() {
        return intg;
    }
    public LongWritable getLong() {
        return lng;
    }

    public void set(Integer intg, Long lng) {
        this.intg.set(intg);
        this.lng.set(lng);
    }

    public void write(DataOutput dataOutput) throws IOException {
        intg.write(dataOutput);
        lng.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        intg.readFields(dataInput);
        lng.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntLongWritablePair that = (IntLongWritablePair) o;

        if (!intg.equals(that.intg)) return false;
        return lng.equals(that.lng);

    }

    @Override
    public int hashCode() {
        int result = intg.hashCode();
        result = 31 * result + lng.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "FloatLongWritablePair{" +
                "flt=" + intg +
                ", lng=" + lng +
                '}';
    }
}
