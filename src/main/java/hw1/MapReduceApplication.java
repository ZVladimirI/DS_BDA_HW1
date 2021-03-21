package hw1;

import hw1.mapper.UtilizationMapper;
import hw1.partitioner.UtilizationPartitioner;
import hw1.reducer.UtilizationReducer;
import hw1.utils.KeyMapper;
import hw1.utils.KeyReducer;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@Log4j
public class MapReduceApplication extends Configured implements Tool {

    /**
     * Entry point for the application
     *
     * @param args Optional arguments: InputDirectory, OutputDirectory, NumberOfReducers AggregationInterval
     * @throws Exception when ToolRunner.run() fails
     */
    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new MapReduceApplication(), args);
        System.exit(result);
    }

    /**
     *
     * @param args additional command line arguments
     * @return 0 if jpb finished successfully
     * @throws Exception when error occured
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        if (args.length != 4) {
            log.error("Usage: InputFileOrDirectory OutputDirectory NumReduceTasks Interval");
            return 2;
        }
        String hdfsInputFileOrDirectory = args[0];
        String hdfsOutputDirectory = args[1];

        configuration.set("interval", args[3]);

        Job job = Job.getInstance(configuration);

        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        job.setMapperClass(UtilizationMapper.class);
        job.setReducerClass(UtilizationReducer.class);
        job.setPartitionerClass(UtilizationPartitioner.class);

        job.setMapOutputKeyClass(KeyMapper.class);
        job.setMapOutputValueClass(FloatWritable.class);


        FileInputFormat.addInputPath(job, new Path(hdfsInputFileOrDirectory));
        FileOutputFormat.setOutputPath(job, new Path((hdfsOutputDirectory)));

        job.setOutputKeyClass(KeyReducer.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setNumReduceTasks(Integer.parseInt(args[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        Counter counter = job.getCounters().findCounter(hw1.utils.Counter.MALFORMED_ROW);
        log.info("=====================COUNTER " + counter.getName() + ": "
                + counter.getValue() + "=====================");
        counter = job.getCounters().findCounter(hw1.utils.Counter.MALFORMED_DEVICE);
        log.info("=====================COUNTER " + counter.getName() + ": "
                + counter.getValue() + "=====================");
        return 0;
    }
}
