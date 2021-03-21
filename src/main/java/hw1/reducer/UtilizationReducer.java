package hw1.reducer;

import hw1.utils.KeyMapper;
import hw1.utils.KeyReducer;
import hw1.utils.MappingReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * Reducer class
 * Input key {@link KeyMapper}
 * Input value {@link FloatWritable}
 * Output key {@link KeyReducer}
 * Output value {@link FloatWritable}
 */
public class UtilizationReducer extends Reducer<KeyMapper, FloatWritable, KeyReducer, FloatWritable> {

    /**
     * Mapping device_id -> device_name
     */
    private Map<Integer, String> mapping;

    /**
     * Initial reducer setup
     * @param context reducer context
     * @throws IOException in MappingReader.read()
     */
    @Override
    protected void setup(Context context) throws IOException {
        URI[] paths = context.getCacheFiles();
        for(URI u : paths) {
            if(u.getPath().toLowerCase().contains("mapping")) {
                mapping = MappingReader.read(context.getConfiguration(), new Path(u.getPath()));
                break;
            }
        }
    }

    /**
     * Reduce function. Calculates average value in given interval
     * @param key key
     * @param values iterable of values
     * @param context reducer context
     * @throws IOException in context.write()
     * @throws InterruptedException in context.write()
     */
    @Override
    protected void reduce(KeyMapper key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0.0F;
        int num = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
            num += 1;
        }
        context.write(new KeyReducer(mapping.get(key.getId()), key.getTimestamp(), key.getInterval()),
                new FloatWritable(sum/num));
    }
}
