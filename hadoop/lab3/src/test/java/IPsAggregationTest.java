import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IPsAggregationTest {
    MapReduceDriver<Object, Text, Text, LongWritable, Text, Text> mapReduceDriver;
    MapDriver<Object, Text, Text, LongWritable> mapDriver;
    ReduceDriver<Text, LongWritable, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        Mapper mapper = new IPsAggregationMapper();
        Reducer reducer = new IPsAggregationReducer();
        mapDriver = new MapDriver<Object, Text, Text, LongWritable>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<Text, LongWritable, Text, Text>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<Object, Text, Text, LongWritable, Text, Text>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("ip32 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss20/floppy.jpg HTTP/1.1\" 200 1000 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\""))
                    .withInput(new LongWritable(1), new Text("ip31 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss20/floppy.jpg HTTP/1.1\" 200 1000 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\""));
        mapDriver.withOutput(new Text("ip32"), new LongWritable(1000))
                    .withOutput(new Text("ip31"), new LongWritable(1000));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(1000));
        values.add(new LongWritable(100));
        reduceDriver.withInput(new Text("ip44"), values);
        reduceDriver.withOutput(new Text("ip44"), new Text("550.00, 1100"));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(1), new Text("ip1 - - [24/Apr/2011:04:23:21 -0400] \"GET /~strabal/grease/photo9/T927-5.jpg HTTP/1.1\" 200 3000 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""))
                        .withInput(new LongWritable(2), new Text("ip4 - - [24/Apr/2011:04:23:54 -0400] \"HEAD /sgi_indigo2/ HTTP/1.0\" 200 200 \"-\" \"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB6.4)\""))
                        .withInput(new LongWritable(3), new Text("ip1 - - [24/Apr/2011:04:23:54 -0400] \"HEAD /sgi_indigo2/ HTTP/1.0\" 200 5000 \"-\" \"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB6.4)\""))
                        .withInput(new LongWritable(4), new Text("ip4 - - [24/Apr/2011:04:23:54 -0400] \"HEAD /sgi_indigo2/ HTTP/1.0\" 200 200 \"-\" \"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB6.4)\""));

        mapReduceDriver.withOutput(new Text("ip1"), new Text("4000.00, 8000"))
                        .withOutput(new Text("ip4"), new Text("200.00, 400"));
        mapReduceDriver.runTest();
    }
}
