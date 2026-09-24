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
 * MapReduce Job 2: Top Source IP Traffic Aggregator
 * Counts occurrences of requests from each source IP address in HDFS.
 * Course: Big Data Analytics (26ECSC404) - KLE Technological University
 */
public class TopSourceIPJob {

    public static class SourceIPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text srcIp = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.startsWith("timestamp") || line.isEmpty()) {
                return;
            }

            // CSV Format: timestamp,src_ip,dst_ip,...
            String[] tokens = line.split(",");
            if (tokens.length > 1) {
                String ip = tokens[1].trim();
                srcIp.set(ip);
                context.write(srcIp, one);
            }
        }
    }

    public static class SourceIPReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
            System.err.println("Usage: TopSourceIPJob <HDFS-Input-Path> <HDFS-Output-Path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Source IP Network Traffic Aggregator");
        job.setJarByClass(TopSourceIPJob.class);

        job.setMapperClass(SourceIPMapper.class);
        job.setCombinerClass(SourceIPReducer.class);
        job.setReducerClass(SourceIPReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
