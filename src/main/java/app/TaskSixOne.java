package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

public class TaskSixOne {

    public static class TaskSixOneStepOneMapper extends Mapper<Object, Text, DoubleWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().trim().split("\t");
            context.write(new DoubleWritable(Double.valueOf(items[1])), new Text(items[0]));
        }
    }

    public static class TaskSixOneStepOneReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }


    public static class TaskSixOneStepTwoMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class TaskSixOneStepTwoReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, new DoubleWritable(Double.valueOf(key.toString())));
            }
        }
    }

    public static class MyComparator extends WritableComparator {
        protected MyComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -Double.compare(Double.valueOf(a.toString()), Double.valueOf(b.toString()));
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 4) {
            throw new RuntimeException();
        }
        // Args: [input] [output] [partition] [inter]

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Task Six Job One Step One");
        job1.setJarByClass(TaskSixOne.class);
        job1.setMapperClass(TaskSixOneStepOneMapper.class);
        job1.setReducerClass(TaskSixOneStepOneReducer.class);
        job1.setMapOutputKeyClass(DoubleWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(DoubleWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));
        job1.setNumReduceTasks(5);
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.totalorderpartitioner.naturalorder", "false");
        Job job2 = Job.getInstance(conf2, "Task Six Job One Step Two");
        job2.setJarByClass(TaskSixOne.class);
        job2.setMapperClass(TaskSixOneStepTwoMapper.class);
        job2.setReducerClass(TaskSixOneStepTwoReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setSortComparatorClass(MyComparator.class);
        job2.setNumReduceTasks(5);
        TotalOrderPartitioner.setPartitionFile(job2.getConfiguration(), new Path(args[2]));
        InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
        InputSampler.writePartitionFile(job2, sampler);
        job2.setPartitionerClass(TotalOrderPartitioner.class);

        job2.waitForCompletion(true);
    }
}
