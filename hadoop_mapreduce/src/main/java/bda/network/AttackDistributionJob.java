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
 * MapReduce Job 3: Attack Category & Security Event Distribution
 * Aggregates frequency of BENIGN vs Attack types (DDoS, PortScan, BruteForce, Botnet).
 * Course: Big Data Analytics (26ECSC404) - KLE Technological University
 */
public class AttackDistributionJob {

    public static class AttackMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text attackType = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.startsWith("timestamp") || line.isEmpty()) {
                return;
            }

            // CSV Format: ...,flag,label (label is token 10)
            String[] tokens = line.split(",");
            if (tokens.length > 10) {
                String label = tokens[10].trim().toUpperCase();
                attackType.set(label);
                context.write(attackType, one);
            }
        }
    }

    public static class AttackReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
            System.err.println("Usage: AttackDistributionJob <HDFS-Input-Path> <HDFS-Output-Path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Network Attack Classification Distribution");
        job.setJarByClass(AttackDistributionJob.class);

        job.setMapperClass(AttackMapper.class);
        job.setCombinerClass(AttackReducer.class);
        job.setReducerClass(AttackReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
