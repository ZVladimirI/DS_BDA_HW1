import hw1.mapper.UtilizationMapper;
import hw1.reducer.UtilizationReducer;
import hw1.utils.KeyMapper;
import hw1.utils.KeyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {
    private final String validRow1 = "1,1510670916247,10.0";

    private MapDriver<LongWritable, Text, KeyMapper, FloatWritable> mapDriver;
    private ReduceDriver<KeyMapper, FloatWritable, KeyReducer, FloatWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, KeyMapper, FloatWritable, KeyReducer, FloatWritable> mapReduceDriver;


    @Before
    public void setup(){
        UtilizationMapper mapper = new UtilizationMapper();
        mapDriver = new MapDriver<>(mapper);
        mapDriver.addCacheFile(new File("src/test/resources/test_mapping").getAbsolutePath());
        Configuration conf = mapDriver.getConfiguration();
        conf.set("interval", "m1");

        UtilizationReducer reducer = new UtilizationReducer();
        reduceDriver = new ReduceDriver<>(reducer);
        reduceDriver.addCacheFile(new File("src/test/resources/test_mapping").getAbsolutePath());
        conf = reduceDriver.getConfiguration();
        conf.set("interval", "m1");

        mapReduceDriver = new MapReduceDriver<>(mapper, reducer);
        mapReduceDriver.addCacheFile(new File("src/test/resources/test_mapping").getAbsolutePath());
        conf = mapReduceDriver.getConfiguration();
        conf.set("interval", "m1");
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(validRow1))
                .withOutput(new KeyMapper(1, 1510670880000L, "1m"),
                        new FloatWritable(((float) 10.0)))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<FloatWritable> iterable = new ArrayList<>();
        iterable.add(new FloatWritable(10.0F));
        iterable.add(new FloatWritable(50.0F));
        reduceDriver
                .withInput(new KeyMapper(1, 1510670880000L, "1m"), iterable)
                .withOutput(new KeyReducer("CPU", 1510670880000L, "1m"),
                        new FloatWritable(30.0F))
                .runTest();
    }

    @Test
    public void testMapperAndReducer() throws IOException {
        String validRow2 = "1,1510670916249,50.0";
        String malformedRow = "aaaa  jnkj lnn";
        String malformedDevice = "198,1510670916247,14.56";
        mapReduceDriver
                .withInput(new LongWritable(), new Text(validRow1))
                .withInput(new LongWritable(), new Text(malformedRow))
                .withInput(new LongWritable(), new Text(validRow2))
                .withInput(new LongWritable(), new Text(malformedDevice))
                .withOutput(new KeyReducer("CPU", 1510670880000L, "1m"),
                        new FloatWritable(30.0F))
                .runTest();
    }
}
