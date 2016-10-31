import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FloatLongWritablePair implements Writable {

    private FloatWritable flt = new FloatWritable(0f);
    private LongWritable lng = new LongWritable(0l);

    public FloatLongWritablePair() {

    }

    public FloatLongWritablePair(Float flt, Long lng) {
        this.flt.set(flt);
        this.lng.set(lng);
    }

    public FloatWritable getFloat() {
        return flt;
    }
    public LongWritable getLong() {
        return lng;
    }

    public void set(Float flt, Long lng) {
        this.flt.set(flt);
        this.lng.set(lng);
    }

    public void write(DataOutput dataOutput) throws IOException {
        flt.write(dataOutput);
        lng.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        flt.readFields(dataInput);
        lng.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FloatLongWritablePair that = (FloatLongWritablePair) o;

        if (!flt.equals(that.flt)) return false;
        return lng.equals(that.lng);

    }

    @Override
    public int hashCode() {
        int result = flt.hashCode();
        result = 31 * result + lng.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "FloatLongWritablePair{" +
                "flt=" + flt +
                ", lng=" + lng +
                '}';
    }
}
