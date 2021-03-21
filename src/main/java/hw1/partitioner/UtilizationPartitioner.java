package hw1.partitioner;

import hw1.utils.KeyMapper;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Custom partitioner to work with custom key  from map phase. Get partitions according to device id.
 * @param <K> any class extends {@link KeyMapper}
 * @param <V> any class for value
 */
public class UtilizationPartitioner<K extends KeyMapper, V> extends Partitioner<K, V> {

    @Override
    public int getPartition(K k, V v, int i) {
        return k.getId() % i;
    }

}
