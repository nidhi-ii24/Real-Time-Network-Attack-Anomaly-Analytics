package bda.network;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce Job 1: Protocol Distribution Counter
 * Aggregates frequency of TCP, UDP, ICMP protocols across historical network logs in HDFS.
 * Course: Big Data Analytics (26ECSC404) - KLE Technological University
 */
public class ProtocolCountJob {

    public static class ProtocolMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text protocol = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            // Skip CSV Header row
            if (line.startsWith("timestamp") || line.isEmpty()) {
                return;
            }

            // CSV Format: timestamp,src_ip,dst_ip,src_port,dst_port,protocol,...
            String[] tokens = line.split(",");
            if (tokens.length > 5) {
                String proto = tokens[5].trim().toUpperCase();
                protocol.set(proto);
                context.write(protocol, one);
            }
        }
    }

    public static class ProtocolReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ProtocolCountJob <HDFS-Input-Path> <HDFS-Output-Path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Network Protocol Distribution Analytics");
        job.setJarByClass(ProtocolCountJob.class);

        job.setMapperClass(ProtocolMapper.class);
        job.setCombinerClass(ProtocolReducer.class); // Local Combiner optimization
        job.setReducerClass(ProtocolReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
