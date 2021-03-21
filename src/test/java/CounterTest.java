import hw1.mapper.UtilizationMapper;
import hw1.utils.Counter;
import hw1.utils.KeyMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CounterTest {
    private final String validRow = "1,1510670916247,10.56";
    private final String malformedRow = "aaaa  jnkj lnn";
    private final String malformedDevice = "198,1510670916247,14.56";

    private MapDriver<LongWritable, Text, KeyMapper, FloatWritable> mapDriver;

    @Before
    public void setup(){
        UtilizationMapper mapper = new UtilizationMapper();
        mapDriver = new MapDriver<>(mapper);
        mapDriver.addCacheFile(new File("src/test/resources/test_mapping").getAbsolutePath());
        Configuration conf = mapDriver.getConfiguration();
        conf.set("interval", "m1");
    }

    @Test
    public void testZeroCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(validRow))
                .withOutput(new KeyMapper(1, 1510670880000L, "1m"),
                        new FloatWritable(((float) 10.56)))
                .runTest();

        assertEquals("MALFORMED_ROW COUNTER", 0,  mapDriver.getCounters()
                .findCounter(Counter.MALFORMED_ROW).getValue());
        assertEquals("MALFORMED_DEVICE COUNTER", 0,  mapDriver.getCounters()
                .findCounter(Counter.MALFORMED_DEVICE).getValue());
    }

    @Test
    public void testMalformedRowCounter() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(malformedRow))
                .runTest();
        assertEquals("MALFORMED_ROW COUNTER", 1,  mapDriver.getCounters()
                .findCounter(Counter.MALFORMED_ROW).getValue());
        assertEquals("MALFORMED_DEVICE COUNTER", 0,  mapDriver.getCounters()
                .findCounter(Counter.MALFORMED_DEVICE).getValue());
    }


    @Test
    public void testMalformedDeviceCounter() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(malformedDevice))
                .runTest();
        assertEquals("MALFORMED_ROW COUNTER", 0,  mapDriver.getCounters()
                .findCounter(Counter.MALFORMED_ROW).getValue());
        assertEquals("MALFORMED_DEVICE COUNTER", 1,  mapDriver.getCounters()
                .findCounter(Counter.MALFORMED_DEVICE).getValue());
    }

    @Test
    public void testCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(malformedRow))
                .withInput(new LongWritable(), new Text(validRow))
                .withInput(new LongWritable(), new Text(malformedDevice))
                .withOutput(new KeyMapper(1, 1510670880000L, "1m"),
                        new FloatWritable(((float) 10.56)))
                .runTest();
        assertEquals("MALFORMED_ROW COUNTER", 1,  mapDriver.getCounters()
                .findCounter(Counter.MALFORMED_ROW).getValue());
        assertEquals("MALFORMED_DEVICE COUNTER", 1,  mapDriver.getCounters()
                .findCounter(Counter.MALFORMED_DEVICE).getValue());
    }
}
