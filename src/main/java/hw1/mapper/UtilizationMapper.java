package hw1.mapper;

import hw1.utils.Counter;
import hw1.utils.KeyMapper;
import hw1.utils.MappingReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * Mapper class
 * Input key {@link LongWritable}
 * Input value {@link Text}
 * Output key {@link KeyMapper}
 * Output value {@link FloatWritable}
 */
public class UtilizationMapper extends Mapper<LongWritable, Text, KeyMapper, FloatWritable> {
    /**
     * Interval in seconds to aggregate data
     */
    private int interval;

    /**
     * String representation of aggregate interval
     */
    private static String intervalParam;

    /**
     * Map of devices to check if the given is valid
     */
    private Map<Integer, String> devices;

    /**
     * Regex pattern to split csv row
     */
    private final static Pattern splitStr = Pattern.compile(",");


    /**
     * Initial mapper setup
     * @param context mapper context
     * @throws IOException if input data parsing fails due to wrong format
     */
    @Override
    protected void setup(Context context) throws IOException{
       String intervalParameter =  context.getConfiguration().get("interval");
       try {
           intervalParam = intervalParameter.substring(1) + intervalParameter.charAt(0);
           String measureType = intervalParameter.substring(0,1);
           int value = Integer.parseInt(intervalParameter.substring(1));
           switch (measureType) {
               case "s":
                   interval = value * 1000;
                   break;
               case "m":
                   interval = value * 60 * 1000;
                   break;
               case "h":
                   interval = value * 3600 * 1000;
                   break;
           }
       }
       catch (RuntimeException noexcept){
           throw new IOException();
       }

        URI[] paths = context.getCacheFiles();
        for(URI u : paths) {
            if(u.getPath().toLowerCase().contains("mapping")) {
                devices = MappingReader.read(context.getConfiguration(), new Path(u.getPath()));
                break;
            }
        }
    }

    /**
     * Map function. Truncates timestamp according to interval for aggregating. Checks data for correctness.
     * Uses counters {@link Counter}
     * @param key input key
     * @param value input value
     * @param context mapper context
     * @throws IOException from context.write()
     * @throws InterruptedException from context.write()
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int id;
        long timestamp;
        float load;
        try {
            String [] data = splitStr.split(value.toString());
            if (data.length == 3) {
                id = Integer.parseInt(data[0]);
                timestamp = Long.parseLong(data[1]);
                timestamp = timestamp - timestamp % interval;
                load = Float.parseFloat(data[2]);
                if (devices.containsKey(id)){
                    context.write(new KeyMapper(id, timestamp, intervalParam), new FloatWritable(load));
                }
                else {
                    context.getCounter(Counter.MALFORMED_DEVICE).increment(1);
                }
            }
            else context.getCounter(Counter.MALFORMED_ROW).increment(1);
        }
        catch (RuntimeException error){
            context.getCounter(Counter.MALFORMED_ROW).increment(1);
        }
    }
}
